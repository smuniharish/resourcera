"""Slow inventory reconciliation using refresh-engine."""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

import anyio
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from refresh_engine import (
    AsyncScheduler,
    DiscoveryResult,
    OverlapPolicy,
    PlanAction,
    RefreshConfig,
    RefreshEngine,
    RefreshMode,
    Resource,
    ResourceSnapshot,
    ResourceSource,
    SchedulerState,
    TimeoutConfig,
    TriggerSource,
)

from resourcera._subprocess import run_worker
from resourcera.accelerators import PlatformAccelerators, merge_platform
from resourcera.config import ResourceConfig
from resourcera.exceptions import ProviderError

if TYPE_CHECKING:
    from refresh_engine import RefreshRequest, RefreshResult, StateStore


class InventoryReport(BaseModel):
    """One inventory discovery and its completeness."""

    model_config = ConfigDict(extra="forbid")

    items: dict[str, object]
    complete: bool
    diagnostics: tuple[str, ...] = ()
    cpu_seconds: float | None = Field(default=None, ge=0, allow_inf_nan=False)


async def probe_inventory(config: ResourceConfig) -> InventoryReport:
    """Run inventory in a bounded child that can be terminated on timeout."""
    output = await run_worker("resourcera._inventory_worker", config)
    try:
        return InventoryReport.model_validate_json(output)
    except ValidationError as exc:
        raise ProviderError(f"inventory worker returned an invalid report: {exc}") from exc


class _InventorySource(ResourceSource):
    def __init__(
        self,
        probe: Callable[[], Awaitable[InventoryReport]],
        config: ResourceConfig,
        on_cost: Callable[[float | None, float], None] | None,
    ) -> None:
        self._probe = probe
        self._on_cost = on_cost
        self._configuration = {
            "scope": config.scope.value,
            "sampling": config.sampling,
            "cpu_memory_interval_seconds": config.cpu_memory_interval_seconds,
            "disk_network_interval_seconds": config.disk_network_interval_seconds,
            "inventory_interval_seconds": config.inventory_interval_seconds,
        }
        self._pending: dict[str, object] = {}
        self.complete: bool | None = None

    async def discover(self) -> DiscoveryResult:
        self.complete = False
        started = time.monotonic()
        report: InventoryReport | None = None
        try:
            report = await self._probe()
        finally:
            if self._on_cost is not None:
                self._on_cost(
                    report.cpu_seconds if report is not None else None, time.monotonic() - started
                )
        items = report.items.copy()
        if "accelerators" in items and "accelerators" in self._pending:
            try:
                previous = PlatformAccelerators.model_validate(self._pending["accelerators"])
                current = PlatformAccelerators.model_validate(items["accelerators"])
                items["accelerators"] = merge_platform(previous, current).model_dump(mode="json")
            except ValidationError as exc:
                raise ProviderError(f"invalid accelerator inventory: {exc}") from exc
        self._pending = {**items, "configuration": self._configuration}
        self.complete = report.complete
        return DiscoveryResult(
            [Resource(name) for name in sorted(self._pending)],
            complete=report.complete,
            diagnostics=report.diagnostics,
        )

    async def snapshot(self, resource: Resource, /) -> ResourceSnapshot:
        return ResourceSnapshot(resource.resource_id, content=self._pending[resource.resource_id])


class InventoryRefresher:
    """Cache inventory with refresh-engine change detection and scheduling."""

    def __init__(
        self,
        config: ResourceConfig,
        *,
        probe: Callable[[], Awaitable[InventoryReport]] | None = None,
        store: StateStore | None = None,
        on_refresh: Callable[[], None] | None = None,
        on_cost: Callable[[float | None, float], None] | None = None,
    ) -> None:
        self._config = config
        self._entries: dict[str, object] = {}
        self._diagnostics: tuple[str, ...] = ()
        self._on_refresh = on_refresh
        self._active = 0
        self._idle = anyio.Event()
        self._idle.set()
        self._needs_full = False
        self._source = _InventorySource(probe or (lambda: probe_inventory(config)), config, on_cost)
        self._engine = RefreshEngine(
            self._source,
            self._apply,
            store=store,
            config=RefreshConfig(
                max_concurrency=1,
                overlap_policy=OverlapPolicy.COALESCE,
                timeout=TimeoutConfig(
                    discovery=config.provider_timeout_seconds + 1,
                    snapshot=config.provider_timeout_seconds,
                    operation=config.provider_timeout_seconds,
                ),
            ),
        )
        self._scheduler = AsyncScheduler(
            self._scheduled_refresh,
            interval=config.inventory_interval_seconds,
        )

    @property
    def entries(self) -> dict[str, object]:
        """Return a shallow copy of the last successfully reconciled inventory."""
        return self._entries.copy()

    @property
    def diagnostics(self) -> tuple[str, ...]:
        """Return diagnostics from the most recent refresh."""
        return self._diagnostics

    @property
    def complete(self) -> bool | None:
        """Report whether the latest discovery was complete."""
        return self._source.complete

    @property
    def retained_objects(self) -> tuple[object, ...]:
        """Return the inventory objects included in managed-state accounting."""
        return self._entries, self._source._pending

    @property
    def needs_full_refresh(self) -> bool:
        """Indicate that shedding requires a full reconciliation before reuse."""
        return self._needs_full

    async def set_paused(self, paused: bool) -> None:
        """Pause or resume periodic inventory without interrupting an active refresh."""
        if paused and self._scheduler.state is SchedulerState.RUNNING:
            await self._scheduler.pause()
        elif not paused and self._scheduler.state is SchedulerState.PAUSED:
            await self._scheduler.resume()

    async def shed_optional(self) -> None:
        """Release optional cached inventories after active reconciliation finishes."""
        await self._idle.wait()
        for name in ("accelerators", "filesystems", "interfaces"):
            self._entries.pop(name, None)
            self._source._pending.pop(name, None)
        self._needs_full = True

    async def start(self) -> RefreshResult:
        """Perform initial discovery and start the refresh scheduler."""
        result = await self.refresh()
        await self._scheduler.start()
        return result

    async def refresh(self) -> RefreshResult:
        """Reconcile inventory without deleting entries on incomplete discovery."""
        return await self._refresh(scheduled=False)

    async def _scheduled_refresh(self) -> RefreshResult:
        return await self._refresh(scheduled=True)

    async def _refresh(self, *, scheduled: bool) -> RefreshResult:
        if self._active == 0:
            self._idle = anyio.Event()
        self._active += 1
        try:
            result = await self._engine.refresh(
                mode=RefreshMode.FULL if self._needs_full else None,
                trigger=TriggerSource.SCHEDULED if scheduled else TriggerSource.MANUAL,
            )
            if self._source.complete and not result.errors:
                self._needs_full = False
            self._record(result)
            return result
        finally:
            self._active -= 1
            if self._active == 0:
                self._idle.set()

    def _record(self, result: RefreshResult) -> None:
        self._diagnostics = result.warnings + tuple(str(issue) for issue in result.errors)
        if self._on_refresh is not None:
            self._on_refresh()

    async def _apply(
        self,
        resource: Resource,
        snapshot: ResourceSnapshot | None,
        action: PlanAction,
        request: RefreshRequest,
        /,
    ) -> None:
        del request
        if action is PlanAction.DELETE:
            self._entries.pop(resource.resource_id, None)
        elif snapshot is not None:
            self._entries[resource.resource_id] = snapshot.content

    async def close(self) -> None:
        """Stop the scheduler before closing its engine."""
        await self._scheduler.stop(cancel=True)
        await self._engine.close(cancel=True)
