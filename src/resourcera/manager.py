"""Async resource-manager lifecycle and cached sampling."""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import sys
import threading
import time
from collections.abc import Awaitable, Callable
from concurrent.futures import Future
from contextlib import AbstractContextManager, suppress
from dataclasses import replace
from types import TracebackType
from typing import TYPE_CHECKING, Literal, Self
from uuid import uuid4

import anyio
from anyio.from_thread import BlockingPortal, start_blocking_portal
from anyio.lowlevel import EventLoopToken
from pydantic import ValidationError

from resourcera.accelerators import (
    DeviceReading,
    PlatformAccelerators,
    ProviderReport,
    merge_platform,
    normalize_device,
    run_platform,
    run_provider,
)
from resourcera.admission import AdmissionCoordinator
from resourcera.config import ResourceConfig
from resourcera.exceptions import (
    ManagerStateError,
    OverheadBudgetError,
    ProviderError,
    ReservationError,
    UnsupportedScopeError,
)
from resourcera.inventory import InventoryRefresher
from resourcera.metrics import SampleClock, metric
from resourcera.models import (
    AcceleratorDevice,
    AdmissionDecision,
    CapacityResult,
    DeviceStatus,
    Metric,
    MetricStatus,
    OverheadMetrics,
    ProviderInfo,
    ResourceLease,
    ResourceSnapshot,
    Scope,
    WorkloadRequirements,
)
from resourcera.overhead import OverheadController, OverheadState, managed_size
from resourcera.telemetry import SystemSampler, Telemetry

if TYPE_CHECKING:
    from refresh_engine import StateStore

_LOG = logging.getLogger(__name__)


def _schedule_in_loop(token: EventLoopToken, callback: Callable[[], None]) -> None:
    native = token.native_token
    scheduler = getattr(native, "call_soon_threadsafe", None)
    if scheduler is None:
        scheduler = getattr(native, "run_sync_soon", None)
    if scheduler is None:
        raise RuntimeError("unsupported event loop for cross-thread notification")
    scheduler(callback)


async def _portal_call[T](portal: BlockingPortal, operation: Callable[[], Awaitable[T]]) -> T:
    future = await anyio.to_thread.run_sync(portal.start_task_soon, operation)
    token = anyio.lowlevel.current_token()
    done = anyio.Event()

    def completed(_future: Future[T]) -> None:
        try:
            _schedule_in_loop(token, done.set)
        except RuntimeError:
            _LOG.debug("Calling event loop closed before portal completion", exc_info=True)

    future.add_done_callback(completed)
    try:
        await done.wait()
        return future.result()
    except BaseException:
        future.cancel()
        raise


def _linux_link_socket() -> socket.socket:
    family = getattr(socket, "AF_NETLINK", None)
    protocol = getattr(socket, "NETLINK_ROUTE", None)
    if family is None or protocol is None:
        raise OSError("netlink route notifications are unsupported")
    sock = socket.socket(family, socket.SOCK_RAW, protocol)
    try:
        sock.bind((0, 1))
        sock.setblocking(False)
    except BaseException:
        sock.close()
        raise
    return sock


class _Runtime:
    """Own all sampler and refresh-engine tasks on one asyncio event loop."""

    def __init__(
        self, config: ResourceConfig, *, bridge: bool, inventory_store: StateStore | None
    ) -> None:
        self.config = config
        self.bridge = bridge
        self._inventory_store = inventory_store
        self._inventory: InventoryRefresher | None = None
        self._coordinator: AdmissionCoordinator | None = None
        self._sampler: SystemSampler | None = None
        self._sample_gate = threading.Lock()
        self._overhead = OverheadController(config)
        self._applied_overhead_state = OverheadState.NORMAL
        self._previous_control_cpu = 0.0
        self._inventory_bootstrap = True
        self._optional_after = 0.0
        self._limiter: anyio.CapacityLimiter | None = None
        self._runner: asyncio.Task[None] | None = None
        self._ready: anyio.Event | None = None
        self._stop: anyio.Event | None = None
        self._wake: anyio.Event | None = None
        self._inventory_event: anyio.Event | None = None
        self._accelerator_lock: anyio.Lock | None = None
        self._providers_idle: anyio.Event | None = None
        self._provider_scopes: set[anyio.CancelScope] = set()
        self._accelerators: tuple[AcceleratorDevice, ...] = ()
        self._platform: PlatformAccelerators | None = None
        self._accelerator_providers: tuple[ProviderInfo, ...] = ()
        self._snapshot: ResourceSnapshot | None = None
        self._startup_error: Exception | None = None
        self._sample_error: str | None = None
        self._event_error: str | None = None
        self._closing = False
        self._host_id: str | None = None
        self._io_requested = False
        self._connections_invalid_before: float | None = None

    async def start(self) -> None:
        self._ready = anyio.Event()
        self._stop = anyio.Event()
        self._wake = anyio.Event()
        self._inventory_event = anyio.Event()
        self._accelerator_lock = anyio.Lock()
        self._providers_idle = anyio.Event()
        self._providers_idle.set()
        self._limiter = anyio.CapacityLimiter(self.config.max_provider_concurrency)
        self._coordinator = AdmissionCoordinator(
            self.config,
            self.snapshot,
            lambda cpu: self._overhead.record_cpu(cpu, time.monotonic()),
            lambda: self._effective_overhead_state() is not OverheadState.SUSPENDED,
        )
        await self._coordinator.start()
        self._inventory = InventoryRefresher(
            self.config,
            store=self._inventory_store,
            on_refresh=self._inventory_changed,
            on_cost=self._inventory_cost,
        )
        self._runner = asyncio.create_task(self._run(), name="resourcera-runtime")
        await self._ready.wait()
        if self._startup_error is not None:
            raise ProviderError(f"resource manager failed to start: {self._startup_error}") from (
                self._startup_error
            )
        if self._snapshot is None:
            raise ProviderError("resource manager stopped before its first sample")

    async def _run(self) -> None:
        assert self._inventory is not None
        assert self._ready is not None
        assert self._stop is not None
        try:
            await self._inventory.start()
            await self._apply_overhead_policy()
            self._inventory_event = anyio.Event()
            await self.refresh_accelerators()
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(self._sample_loop)
                tasks.start_soon(self._accelerator_loop)
                assert self._coordinator is not None
                tasks.start_soon(self._coordinator.supervise)
                if sys.platform == "linux":
                    tasks.start_soon(self._watch_links)
                await self._stop.wait()
                tasks.cancel_scope.cancel()
        except Exception as exc:
            self._startup_error = exc
            _LOG.exception("Resource manager runtime stopped unexpectedly")
        finally:
            try:
                with anyio.CancelScope(shield=True):
                    try:
                        if self._coordinator is not None:
                            await self._coordinator.close()
                    finally:
                        await self._inventory.close()
            finally:
                self._ready.set()

    def _inventory_changed(self) -> None:
        if self._inventory_event is not None:
            self._inventory_event.set()

    def _inventory_cost(self, cpu_seconds: float | None, duration: float) -> None:
        previous = self._overhead.state
        self._overhead.record_provider(
            cpu_seconds,
            duration,
            time.monotonic(),
            accelerator=False,
            bootstrap=self._inventory_bootstrap,
        )
        self._inventory_bootstrap = False
        if self._overhead.state is not previous and self._wake is not None:
            self._wake.set()

    def _effective_overhead_state(self, now: float | None = None) -> OverheadState:
        if now is None:
            now = time.monotonic()
        state = self._overhead.evaluate(now)
        if state is OverheadState.NORMAL and now < self._optional_after:
            return OverheadState.REDUCED
        return state

    async def _apply_overhead_policy(self, sample: Telemetry | None = None) -> None:
        assert self._inventory is not None
        if sample is not None and (
            self._overhead.managed_bytes >= self.config.managed_state_budget_bytes
            or not self._overhead.size_complete
        ):
            await self._inventory.set_paused(True)
            await self._shed_optional_state(sample)
        state = self._effective_overhead_state()
        if state is not OverheadState.NORMAL:
            self._stale_optional_accelerators(state)
        if state is OverheadState.SUSPENDED:
            if self._applied_overhead_state is not OverheadState.SUSPENDED:
                self._connections_invalid_before = time.monotonic()
            if self._snapshot is not None:
                self._snapshot = self._snapshot.model_copy(
                    update={
                        "network_connections": self._stale_connections(
                            self._snapshot.network_connections
                        )
                    }
                )
        await self._inventory.set_paused(
            state is OverheadState.SUSPENDED or time.monotonic() < self._optional_after
        )
        if (
            state is OverheadState.NORMAL
            and self._applied_overhead_state is not OverheadState.NORMAL
        ):
            if self._inventory.needs_full_refresh:
                await self._inventory.refresh()
            self._inventory_changed()
        self._applied_overhead_state = state

    @staticmethod
    def _stale_connections(reading: Metric[int]) -> Metric[int]:
        if reading.measurement_status not in {MetricStatus.AVAILABLE, MetricStatus.ESTIMATED}:
            return reading
        return reading.model_copy(
            update={
                "measurement_status": MetricStatus.STALE,
                "detail": "Connection sampling was suspended by the overhead policy.",
            }
        )

    def _stale_optional_accelerators(self, state: OverheadState) -> None:
        def stale(device: AcceleratorDevice) -> AcceleratorDevice:
            if state is OverheadState.REDUCED and device.source in {
                "linux-sysfs",
                "windows-cfgmgr32",
                "macos-iokit",
            }:
                return device
            updates: dict[str, object] = {}
            for name in (
                "memory_total_bytes",
                "memory_used_bytes",
                "utilization_percent",
                "process_memory_bytes",
            ):
                reading = getattr(device, name)
                if reading.measurement_status in {MetricStatus.AVAILABLE, MetricStatus.ESTIMATED}:
                    updates[name] = reading.model_copy(
                        update={
                            "measurement_status": MetricStatus.STALE,
                            "detail": "Optional probing is paused by the overhead policy.",
                        }
                    )
            if not updates:
                return device
            updates["statuses"] = tuple(
                dict.fromkeys((*device.statuses, DeviceStatus.PARTIAL_TELEMETRY))
            )
            return device.model_copy(update=updates)

        updated = tuple(stale(device) for device in self._accelerators)
        if updated != self._accelerators:
            self._accelerators = updated
            if self._snapshot is not None:
                self._snapshot = self._snapshot.model_copy(update={"accelerators": updated})

    async def _shed_optional_state(self, sample: Telemetry) -> None:
        assert self._inventory is not None
        assert self._snapshot is not None
        await self._inventory.shed_optional()
        if self._sampler is not None and self._sample_gate.acquire(blocking=False):
            try:
                self._sampler.shed_optional()
            finally:
                self._sample_gate.release()
        self._platform = None
        self._accelerators = ()
        self._io_requested = True
        self._optional_after = time.monotonic() + min(
            30.0, self.config.cpu_budget_window_seconds / 2
        )
        self._connections_invalid_before = time.monotonic()
        message = "Detailed inventory was evicted by the managed-state budget."

        def partial(provider: ProviderInfo) -> ProviderInfo:
            statuses = tuple(dict.fromkeys((*provider.statuses, DeviceStatus.PARTIAL_TELEMETRY)))
            detail = f"{provider.detail}; {message}" if provider.detail else message
            return provider.model_copy(update={"statuses": statuses, "detail": detail})

        self._accelerator_providers = tuple(partial(item) for item in self._accelerator_providers)
        providers = tuple(partial(item) for item in self._snapshot.providers)
        disk = self._snapshot.disk.model_copy(update={"filesystems": ()})
        snapshot = self._snapshot.model_copy(
            update={
                "disk": disk,
                "network": (),
                "network_connections": self._stale_connections(self._snapshot.network_connections),
                "accelerators": (),
                "providers": providers,
            }
        )
        sampler_objects = self._sampler.retained_objects if self._sampler is not None else ()
        size = managed_size(
            snapshot,
            self._inventory.retained_objects,
            sampler_objects,
            self._accelerator_providers,
            self._overhead.retained_objects,
            self._coordinator.retained_objects if self._coordinator is not None else (),
        )
        self._overhead.set_managed_size(size, time.monotonic())
        self._snapshot = snapshot.model_copy(
            update={"overhead": self._overhead_metrics(sample, managed_known=True)}
        )

    async def _run_accelerator_provider(self, kind: str, argument: str) -> ProviderReport:
        started = time.monotonic()
        cpu_seconds: float | None = None
        try:
            report = await run_provider(kind, argument, self.config)
            cpu_seconds = report.cpu_seconds
            return report
        finally:
            now = time.monotonic()
            self._overhead.record_provider(cpu_seconds, now - started, now, accelerator=True)

    async def _run_platform_provider(self) -> PlatformAccelerators:
        started = time.monotonic()
        cpu_seconds: float | None = None
        try:
            platform = await run_platform(self.config)
            cpu_seconds = platform.cpu_seconds
            return platform
        finally:
            now = time.monotonic()
            self._overhead.record_provider(cpu_seconds, now - started, now, accelerator=True)

    async def _accelerator_loop(self) -> None:
        while True:
            assert self._inventory_event is not None
            await self._inventory_event.wait()
            self._inventory_event = anyio.Event()
            try:
                await self.refresh_accelerators()
            except ManagerStateError:
                if self._closing:
                    return
                raise

    async def refresh_accelerators(self, *, rescan: bool = False) -> tuple[AcceleratorDevice, ...]:
        """Refresh accelerator details independently of cached snapshot reads."""
        assert self._providers_idle is not None
        if self._closing:
            raise ManagerStateError("the resource manager is closing")
        if rescan and (
            self._effective_overhead_state() is OverheadState.SUSPENDED
            or time.monotonic() < self._optional_after
        ):
            raise OverheadBudgetError("accelerator probes are suspended by the overhead budget")
        with anyio.CancelScope() as scope:
            if not self._provider_scopes:
                self._providers_idle = anyio.Event()
            self._provider_scopes.add(scope)
            try:
                return await self._refresh_accelerators(rescan=rescan)
            finally:
                self._provider_scopes.remove(scope)
                if not self._provider_scopes:
                    self._providers_idle.set()
        raise ManagerStateError("accelerator refresh was cancelled during manager shutdown")

    async def _refresh_accelerators(self, *, rescan: bool) -> tuple[AcceleratorDevice, ...]:
        assert self._inventory is not None
        assert self._accelerator_lock is not None
        async with self._accelerator_lock:
            try:
                if rescan:
                    platform = await self._run_platform_provider()
                else:
                    platform = PlatformAccelerators.model_validate(
                        self._inventory.entries.get("accelerators")
                    )
            except (ProviderError, ValidationError) as exc:
                platform = PlatformAccelerators(
                    devices=(),
                    complete=False,
                    diagnostics=(f"accelerator inventory unavailable: {exc}",),
                    nvidia_installed=None,
                )
            platform = merge_platform(self._platform, platform)
            self._platform = platform
            os_statuses = [DeviceStatus.PROVIDER_AVAILABLE]
            if platform.devices:
                os_statuses.append(DeviceStatus.DEVICE_DETECTED)
            elif platform.complete:
                os_statuses.append(DeviceStatus.DEVICE_NOT_DETECTED)
            if not platform.complete:
                os_statuses.append(DeviceStatus.PARTIAL_TELEMETRY)
            providers = [
                ProviderInfo(
                    name="operating-system accelerator discovery",
                    statuses=tuple(os_statuses),
                    supported_vendors=("NVIDIA", "AMD", "Intel", "Apple", "Google"),
                    supported_device_types=(
                        "gpu",
                        "accelerator",
                        "display-controller",
                        "tpu",
                        "npu",
                    ),
                    supported_operating_systems=("Linux", "Windows", "macOS"),
                    supported_metrics=("device identity", "AMD GPU memory", "AMD GPU utilization"),
                    sampling_supported=True,
                    limitations=(
                        "OS-visible display or PCI devices do not establish compute accessibility.",
                        "TPU and NPU types require a recognized device ID or kernel driver.",
                    ),
                    detail="; ".join(platform.diagnostics) or None,
                )
            ]
            reports: list[ProviderReport] = []
            allow_optional = self._effective_overhead_state() is OverheadState.NORMAL or (
                rescan and self._effective_overhead_state() is OverheadState.REDUCED
            )
            if not allow_optional:
                providers.append(
                    ProviderInfo(
                        name="NVIDIA NVML",
                        statuses=(DeviceStatus.PARTIAL_TELEMETRY,),
                        supported_vendors=("NVIDIA",),
                        sampling_supported=True,
                        detail="Optional accelerator probing is paused by the overhead policy.",
                    )
                )
            elif (
                platform.nvidia_installed is True
                and platform.complete
                and not platform.nvidia_interface_detected
                and not any(device.vendor == "NVIDIA" for device in platform.devices)
            ):
                providers.append(
                    ProviderInfo(
                        name="NVIDIA NVML",
                        statuses=(DeviceStatus.DEVICE_NOT_DETECTED,),
                        supported_vendors=("NVIDIA",),
                        sampling_supported=True,
                        detail="NVML was not initialized because no NVIDIA interface was detected.",
                    )
                )
            elif platform.nvidia_installed is True:
                try:
                    reports.append(await self._run_accelerator_provider("nvidia", str(os.getpid())))
                except ProviderError as exc:
                    providers.append(
                        ProviderInfo(
                            name="NVIDIA NVML",
                            statuses=(DeviceStatus.PROVIDER_ERROR,),
                            required_dependencies=("nvidia-ml-py",),
                            detail=str(exc),
                        )
                    )
            else:
                status = (
                    DeviceStatus.PROVIDER_NOT_INSTALLED
                    if platform.nvidia_installed is False
                    else DeviceStatus.PROVIDER_ERROR
                )
                providers.append(
                    ProviderInfo(
                        name="NVIDIA NVML",
                        statuses=(status,),
                        supported_vendors=("NVIDIA",),
                        supported_device_types=("gpu",),
                        required_dependencies=("nvidia-ml-py",),
                        sampling_supported=True,
                        detail=(
                            "Install the optional resourcera[nvidia] extra for NVML telemetry."
                            if status is DeviceStatus.PROVIDER_NOT_INSTALLED
                            else "NVML interface availability could not be established."
                        ),
                    )
                )
            for path in self.config.accelerator_providers:
                if not allow_optional or self._overhead.state is OverheadState.SUSPENDED:
                    providers.append(
                        ProviderInfo(
                            name=path,
                            statuses=(DeviceStatus.PARTIAL_TELEMETRY,),
                            detail="Optional accelerator probing is paused by the overhead policy.",
                        )
                    )
                    continue
                try:
                    reports.append(await self._run_accelerator_provider("custom", path))
                except ProviderError as exc:
                    providers.append(
                        ProviderInfo(
                            name=path,
                            statuses=(DeviceStatus.PROVIDER_ERROR,),
                            detail=str(exc),
                        )
                    )
            readings: list[tuple[DeviceReading, SampleClock]] = []
            preserved = (
                tuple(
                    device
                    for device in self._accelerators
                    if device.source not in {"linux-sysfs", "windows-cfgmgr32", "macos-iokit"}
                )
                if not allow_optional
                else ()
            )
            observed = {device.id for device in preserved}
            for report in reports:
                provider = report.provider
                for reading in report.devices:
                    if reading.id in observed:
                        provider = provider.model_copy(
                            update={
                                "statuses": (*provider.statuses, DeviceStatus.PARTIAL_TELEMETRY),
                                "detail": f"duplicate device identity ignored: {reading.id}",
                            }
                        )
                        continue
                    observed.add(reading.id)
                    readings.append(
                        (reading, SampleClock(reading.sampled_at, reading.sampled_monotonic))
                    )
                providers.append(provider)
            nvidia_info = next((p for p in providers if p.name == "NVIDIA NVML"), None)
            for reading in platform.devices:
                if reading.id in observed:
                    continue
                statuses = list(reading.statuses)
                detail = reading.detail or ""
                if reading.vendor == "NVIDIA" and nvidia_info is not None:
                    if DeviceStatus.PROVIDER_NOT_INSTALLED in nvidia_info.statuses:
                        statuses.append(DeviceStatus.PROVIDER_NOT_INSTALLED)
                    elif DeviceStatus.PROVIDER_ERROR in nvidia_info.statuses:
                        statuses.append(DeviceStatus.PROVIDER_ERROR)
                    else:
                        statuses.append(DeviceStatus.PARTIAL_TELEMETRY)
                        detail += "; not correlated with an NVML device and may be a duplicate"
                elif not (
                    DeviceStatus.TELEMETRY_AVAILABLE in statuses
                    or DeviceStatus.PARTIAL_TELEMETRY in statuses
                ):
                    statuses.append(DeviceStatus.DEVICE_UNSUPPORTED)
                readings.append(
                    (
                        reading.model_copy(update={"statuses": tuple(statuses), "detail": detail}),
                        SampleClock(reading.sampled_at, reading.sampled_monotonic),
                    )
                )
            self._accelerators = preserved + tuple(
                normalize_device(
                    reading,
                    clock,
                    (
                        self.config.inventory_interval_seconds * 2
                        if reading.source in {"linux-sysfs", "windows-cfgmgr32", "macos-iokit"}
                        else self.config.cpu_memory_interval_seconds * 2
                    ),
                )
                for reading, clock in readings
            )
            self._accelerator_providers = tuple(providers)
            if self._snapshot is not None:
                base = tuple(
                    item
                    for item in self._snapshot.providers
                    if item.name in {"refresh-engine inventory", "psutil"}
                )
                self._snapshot = self._snapshot.model_copy(
                    update={
                        "accelerators": self._accelerators,
                        "providers": base + self._accelerator_providers,
                    }
                )
            await self._apply_overhead_policy()
            return self._accelerators

    async def rescan_accelerators(self) -> tuple[AcceleratorDevice, ...]:
        return await self.refresh_accelerators(rescan=True)

    def _sample_blocking(self, sample_io: bool) -> Telemetry:
        if not self._sample_gate.acquire(blocking=False):
            raise ProviderError("the previous system sampling call is still running")
        try:
            started = time.thread_time()
            if self._sampler is None:
                self._sampler = SystemSampler(self.config)
                self._host_id = socket.gethostname()
            sample = self._sampler.sample(sample_io=sample_io)
            return replace(sample, cpu_seconds=time.thread_time() - started)
        finally:
            self._sample_gate.release()

    async def _sample_loop(self) -> None:
        assert self._ready is not None
        assert self._limiter is not None
        next_io = 0.0
        samples = 0
        while True:
            do_io = (time.monotonic() >= next_io or self._io_requested) and (
                self._effective_overhead_state() is not OverheadState.SUSPENDED
                and time.monotonic() >= self._optional_after
            )
            try:
                if self._sample_gate.locked():
                    raise ProviderError("the previous system sampling call is still running")
                with anyio.fail_after(self.config.provider_timeout_seconds):
                    sample = await anyio.to_thread.run_sync(
                        self._sample_blocking,
                        do_io,
                        abandon_on_cancel=True,
                        limiter=self._limiter,
                    )
                if do_io:
                    next_io = time.monotonic() + self.config.disk_network_interval_seconds
                    self._io_requested = False
                self._sample_error = None
                self._overhead.record_cpu(
                    sample.cpu_seconds + self._previous_control_cpu,
                    time.monotonic(),
                    bootstrap=samples == 0,
                )
                self._publish(sample)
                samples += 1
                await self._apply_overhead_policy(sample)
                del sample
            except Exception as exc:
                self._sample_error = f"{type(exc).__name__}: {exc}"
                _LOG.error("Resource sampling failed: %s", self._sample_error)
                if self._snapshot is None:
                    self._startup_error = exc
                else:
                    self._mark_sampling_error()
            finally:
                self._ready.set()
            assert self._wake is not None
            interval = self._overhead.interval()
            if time.monotonic() < self._optional_after:
                interval = max(interval, self.config.cpu_memory_interval_seconds * 2)
            if samples == 1:
                interval = min(interval, self.config.cpu_memory_interval_seconds)
            if self._snapshot is not None:
                pressure = (
                    self._snapshot.pressure.cpu_percent,
                    self._snapshot.pressure.memory_percent,
                    self._snapshot.pressure.io_percent,
                )
                if any(
                    item.status in {MetricStatus.AVAILABLE, MetricStatus.ESTIMATED}
                    and item.value is not None
                    and item.value >= 20
                    for item in pressure
                ):
                    interval = max(interval, self.config.cpu_memory_interval_seconds * 2)
            with anyio.move_on_after(interval):
                await self._wake.wait()
            self._wake = anyio.Event()

    async def _watch_links(self) -> None:
        assert self._inventory is not None
        try:
            sock = await anyio.to_thread.run_sync(_linux_link_socket)
        except OSError as exc:
            self._event_error = f"link-change notifications unavailable: {exc}"
            _LOG.warning("%s; periodic inventory refresh remains active", self._event_error)
            return
        try:
            while True:
                await anyio.wait_readable(sock)
                try:
                    sock.recv(65536)
                except BlockingIOError:
                    continue
                await anyio.sleep(0.5)
                for _ in range(16):
                    try:
                        sock.recv(65536)
                    except BlockingIOError:
                        break
                if self._effective_overhead_state() is not OverheadState.NORMAL:
                    continue
                await self._inventory.refresh()
                self._io_requested = True
                if self._wake is not None:
                    self._wake.set()
        except OSError as exc:
            self._event_error = f"link-change notifications failed: {exc}"
            _LOG.warning("%s; periodic inventory refresh remains active", self._event_error)
        finally:
            sock.close()

    def _providers(self, sample: Telemetry) -> tuple[ProviderInfo, ...]:
        assert self._inventory is not None
        details = self._inventory.diagnostics
        if self._event_error:
            details += (self._event_error,)
        inventory_states = [DeviceStatus.PROVIDER_AVAILABLE]
        if self._inventory.complete is False:
            inventory_states.append(DeviceStatus.PARTIAL_TELEMETRY)
        sample_states = [DeviceStatus.PROVIDER_AVAILABLE, DeviceStatus.TELEMETRY_AVAILABLE]
        if sample.warnings:
            sample_states.append(DeviceStatus.PARTIAL_TELEMETRY)
        return (
            ProviderInfo(
                name="refresh-engine inventory",
                statuses=tuple(inventory_states),
                supported_metrics=("hardware inventory", "topology", "configuration"),
                detail="; ".join(details) or None,
            ),
            ProviderInfo(
                name="psutil",
                statuses=tuple(sample_states),
                supported_metrics=("cpu", "memory", "disk", "network"),
                detail="; ".join(sample.warnings) or None,
            ),
        )

    def _overhead_metrics(self, sample: Telemetry, *, managed_known: bool) -> OverheadMetrics:
        clock = SampleClock.now()
        freshness = self.config.cpu_memory_interval_seconds * 2
        unknown = self._overhead.unmeasured_provider_work
        state = self._effective_overhead_state(clock.monotonic)
        return OverheadMetrics(
            cpu_seconds=metric(
                self._overhead.cpu_seconds(clock.monotonic),
                "CPU seconds/window",
                "resourcera sampler and completed child probes",
                Scope.PROCESS,
                clock,
                freshness,
                status=MetricStatus.ESTIMATED,
                detail=(
                    "Attributable work only; some failed or timed-out provider work was unmeasured."
                    if unknown
                    else "Attributable sampler, control, and completed child-process CPU time."
                ),
            ),
            bootstrap_cpu_seconds=metric(
                self._overhead.bootstrap_seconds(clock.monotonic),
                "CPU seconds/window",
                "resourcera bounded startup allowance",
                Scope.PROCESS,
                clock,
                freshness,
                detail="One-time allowance is capped at one CPU-budget window or one CPU second.",
            ),
            cpu_budget_seconds=(
                self.config.cpu_budget_fraction * self.config.cpu_budget_window_seconds
            ),
            managed_state_bytes=metric(
                self._overhead.managed_bytes if managed_known else None,
                "bytes",
                "resourcera managed Python objects",
                Scope.PROCESS,
                clock,
                freshness,
                status=MetricStatus.ESTIMATED if managed_known else MetricStatus.UNAVAILABLE,
                detail=(
                    "Estimated selected object graphs; native allocations and engine internals "
                    "are excluded."
                    if self._overhead.size_complete
                    else "Traversal limit reached; the overhead policy treats size as over budget."
                ),
            ),
            owned_tasks=metric(
                6 if sys.platform == "linux" else 5,
                "tasks",
                "resourcera",
                Scope.PROCESS,
                clock,
                freshness,
                status=MetricStatus.ESTIMATED,
                detail=(
                    "Runtime, sampler, accelerator watcher, scheduler, lease supervisor, "
                    "and Linux link watcher."
                ),
            ),
            owned_threads=metric(
                1 if self.bridge else 0,
                "threads",
                "resourcera",
                Scope.PROCESS,
                clock,
                freshness,
                status=MetricStatus.ESTIMATED,
                detail="dedicated bridge threads; shared AnyIO workers are not exclusively owned",
            ),
            sampling_duration_seconds=metric(
                sample.duration_seconds,
                "seconds",
                "resourcera sampling thread",
                Scope.PROCESS,
                clock,
                freshness,
            ),
            provider_cpu_seconds=metric(
                self._overhead.last_provider_cpu,
                "CPU seconds/probe",
                "resourcera child process",
                Scope.PROCESS,
                clock,
                freshness,
                detail="CPU is unavailable for failed or terminated probes.",
            ),
            provider_duration_seconds=metric(
                self._overhead.last_provider_duration,
                "seconds/probe",
                "resourcera",
                Scope.PROCESS,
                clock,
                freshness,
            ),
            accelerator_poll_cpu_seconds=metric(
                self._overhead.last_accelerator_cpu,
                "CPU seconds/probe",
                "resourcera accelerator child",
                Scope.PROCESS,
                clock,
                freshness,
                detail="CPU is unavailable for failed or terminated probes.",
            ),
            disk_activity_bytes=metric(
                None,
                "bytes",
                "operating system",
                Scope.PROCESS,
                clock,
                freshness,
                status=MetricStatus.UNAVAILABLE,
                detail="Disk activity cannot be attributed reliably to this library.",
            ),
            network_activity_bytes=metric(
                None,
                "bytes",
                "operating system",
                Scope.PROCESS,
                clock,
                freshness,
                status=MetricStatus.UNAVAILABLE,
                detail="Network activity cannot be attributed reliably to this library.",
            ),
            state=state.value,
            degraded=state is not OverheadState.NORMAL,
        )

    def _publish(self, sample: Telemetry) -> None:
        started = time.thread_time()
        clock = sample.clock
        connections = sample.network_connections
        if self._connections_invalid_before is not None:
            if connections.sampled_monotonic <= self._connections_invalid_before:
                connections = self._stale_connections(connections)
            else:
                self._connections_invalid_before = None
        snapshot = ResourceSnapshot(
            id=str(uuid4()),
            timestamp=clock.timestamp,
            scope=self.config.scope,
            host_id=self._host_id,
            process_id=os.getpid(),
            container_id=sample.container_id,
            cpu=sample.cpu,
            memory=sample.memory,
            disk=sample.disk,
            network=sample.network,
            network_connections=connections,
            accelerators=self._accelerators,
            pressure=sample.pressure,
            providers=self._providers(sample) + self._accelerator_providers,
            overhead=self._overhead_metrics(sample, managed_known=False),
            sampling_duration_seconds=sample.duration_seconds,
        )
        inventory = self._inventory.retained_objects if self._inventory is not None else ()
        sampler = self._sampler.retained_objects if self._sampler is not None else ()
        size = managed_size(
            snapshot,
            inventory,
            sampler,
            self._platform,
            self._accelerator_providers,
            self._overhead.retained_objects,
            self._coordinator.retained_objects if self._coordinator is not None else (),
        )
        self._overhead.set_managed_size(size, time.monotonic())
        self._snapshot = snapshot.model_copy(
            update={"overhead": self._overhead_metrics(sample, managed_known=True)}
        )
        self._previous_control_cpu = time.thread_time() - started

    def _mark_sampling_error(self) -> None:
        assert self._snapshot is not None
        providers = tuple(
            provider.model_copy(
                update={"statuses": (DeviceStatus.PROVIDER_ERROR,), "detail": self._sample_error}
            )
            if provider.name == "psutil"
            else provider
            for provider in self._snapshot.providers
        )
        self._snapshot = self._snapshot.model_copy(update={"providers": providers})

    def snapshot(self) -> ResourceSnapshot:
        if self._startup_error is not None:
            raise ProviderError(f"resource manager runtime failed: {self._startup_error}") from (
                self._startup_error
            )
        if self._snapshot is None:
            raise ManagerStateError("no resource snapshot is available")
        return self._snapshot

    def coordinator(self) -> AdmissionCoordinator:
        """Return the initialized reservation coordinator."""
        if self._coordinator is None:
            raise ManagerStateError("reservation coordinator is not running")
        return self._coordinator

    async def close(self) -> None:
        runner = self._runner
        self._closing = True
        try:
            if self._coordinator is not None:
                await self._coordinator.close()
        finally:
            if self._stop is not None:
                self._stop.set()
            for scope in tuple(self._provider_scopes):
                scope.cancel()
            if runner is not None:
                if not runner.done():
                    runner.cancel()
                with suppress(asyncio.CancelledError):
                    await runner
            if self._providers_idle is not None:
                await self._providers_idle.wait()


class ResourceManager:
    """Maintain cached resource observations without blocking the application loop."""

    def __init__(
        self,
        config: ResourceConfig | None = None,
        *,
        scope: Scope | str | None = None,
        sampling: Literal["hybrid"] | None = None,
        overhead_policy: Literal["strict"] | None = None,
        inventory_store: StateStore | None = None,
    ) -> None:
        settings = config.model_dump() if config is not None else {}
        if scope is not None:
            settings["scope"] = scope
        if sampling is not None:
            settings["sampling"] = sampling
        if overhead_policy is not None:
            settings["overhead_policy"] = overhead_policy
        self.config = ResourceConfig.model_validate(settings)
        if self.config.scope is Scope.DISTRIBUTED:
            raise UnsupportedScopeError("distributed discovery has no configured backend")
        self._state = "new"
        self._inventory_store = inventory_store
        self._lock = anyio.Lock()
        self._runtime: _Runtime | None = None
        self._portal_context: AbstractContextManager[BlockingPortal] | None = None
        self._portal: BlockingPortal | None = None

    async def start(self) -> None:
        """Start monitoring and wait for the first cached observation."""
        async with self._lock:
            if self._state == "running":
                return
            if self._state != "new":
                raise ManagerStateError(f"cannot start a manager in state {self._state!r}")
            self._state = "starting"
            try:
                try:
                    asyncio.get_running_loop()
                    bridge = False
                except RuntimeError:
                    bridge = True
                self._runtime = _Runtime(
                    self.config, bridge=bridge, inventory_store=self._inventory_store
                )
                if bridge:
                    self._portal_context = start_blocking_portal(
                        backend="asyncio", name="resourcera-asyncio-bridge"
                    )
                    self._portal = await anyio.to_thread.run_sync(self._portal_context.__enter__)
                    await _portal_call(self._portal, self._runtime.start)
                else:
                    await self._runtime.start()
                self._state = "running"
            except BaseException:
                with anyio.CancelScope(shield=True):
                    await self._shutdown()
                    self._state = "closed"
                raise

    async def snapshot(self) -> ResourceSnapshot:
        """Return the most recent in-memory snapshot; no discovery is performed."""
        if self._state != "running" or self._runtime is None:
            raise ManagerStateError("the resource manager is not running")
        return self._runtime.snapshot()

    async def _call_runtime[T](self, operation: Callable[[_Runtime], Awaitable[T]]) -> T:
        if self._state != "running" or self._runtime is None:
            raise ManagerStateError("the resource manager is not running")
        runtime = self._runtime
        if self._portal is not None:
            return await _portal_call(self._portal, lambda: operation(runtime))
        return await operation(runtime)

    async def capacity(
        self, requirements: WorkloadRequirements | dict[str, object] | None = None
    ) -> CapacityResult:
        """Estimate capacity from the cached snapshot without running discovery."""
        request = (
            requirements
            if requirements is None or isinstance(requirements, WorkloadRequirements)
            else WorkloadRequirements.model_validate(requirements)
        )
        return await self._call_runtime(lambda runtime: runtime.coordinator().capacity(request))

    async def reserve(
        self,
        requirements: WorkloadRequirements | dict[str, object],
        *,
        lease_id: str | None = None,
        wait: bool = False,
        wait_timeout_seconds: float | None = None,
        auto_renew: bool = True,
    ) -> ResourceLease:
        """Acquire a logical lease, rejecting immediately unless bounded waiting is requested."""
        request = (
            requirements
            if isinstance(requirements, WorkloadRequirements)
            else WorkloadRequirements.model_validate(requirements)
        )
        actual_id = str(uuid4()) if lease_id is None else lease_id
        attempt_id = str(uuid4())
        delivery = threading.Event()
        duration = (
            self.config.admission_wait_timeout_seconds
            if wait_timeout_seconds is None
            else wait_timeout_seconds
        )
        runtime = self._runtime
        portal = self._portal
        try:
            lease = await self._call_runtime(
                lambda active: active.coordinator().reserve(
                    request,
                    lease_id=actual_id,
                    wait=wait,
                    wait_timeout_seconds=duration,
                    auto_renew=auto_renew,
                    attempt_id=attempt_id,
                    delivery=delivery,
                )
            )
            delivery.set()
            return lease
        except BaseException:
            if runtime is not None:
                with anyio.CancelScope(shield=True):
                    try:
                        if portal is not None:
                            await _portal_call(
                                portal,
                                lambda: runtime.coordinator().compensate(actual_id, attempt_id),
                            )
                        else:
                            await runtime.coordinator().compensate(actual_id, attempt_id)
                    except (ManagerStateError, ReservationError, RuntimeError):
                        _LOG.exception("Could not compensate an interrupted reservation attempt")
            raise

    async def reservation(self, lease: ResourceLease | str) -> ResourceLease:
        """Return the latest owned lease, or report its loss or expiration."""
        lease_id = lease.id if isinstance(lease, ResourceLease) else lease
        return await self._call_runtime(lambda runtime: runtime.coordinator().inspect(lease_id))

    async def renew(self, lease: ResourceLease | str) -> ResourceLease:
        """Renew an owned lease before its expiration."""
        lease_id = lease.id if isinstance(lease, ResourceLease) else lease
        return await self._call_runtime(lambda runtime: runtime.coordinator().renew(lease_id))

    async def release(self, lease: ResourceLease | str) -> None:
        """Release an owned lease; repeating a successful release is harmless."""
        lease_id = lease.id if isinstance(lease, ResourceLease) else lease
        await self._call_runtime(lambda runtime: runtime.coordinator().release(lease_id))

    def admission(
        self,
        requirements: WorkloadRequirements | dict[str, object],
        *,
        wait: bool = False,
        wait_timeout_seconds: float | None = None,
    ) -> _AdmissionContext:
        """Create a cancellation-safe context for resource-aware work."""
        return _AdmissionContext(
            self, requirements, wait=wait, wait_timeout_seconds=wait_timeout_seconds
        )

    async def refresh_accelerators(self) -> tuple[AcceleratorDevice, ...]:
        """Perform bounded on-demand accelerator discovery and telemetry."""
        if self._state != "running" or self._runtime is None:
            raise ManagerStateError("the resource manager is not running")
        if self._portal is not None:
            return await _portal_call(self._portal, self._runtime.rescan_accelerators)
        return await self._runtime.rescan_accelerators()

    async def _shutdown(self) -> None:
        try:
            if self._runtime is not None:
                if self._portal is not None:
                    await _portal_call(self._portal, self._runtime.close)
                else:
                    await self._runtime.close()
        finally:
            if self._portal_context is not None and self._portal is not None:
                context = self._portal_context
                await anyio.to_thread.run_sync(lambda: context.__exit__(None, None, None))
                self._portal = None
                self._portal_context = None

    async def close(self) -> None:
        """Stop owned tasks and release the optional backend bridge."""
        with anyio.CancelScope(shield=True):
            async with self._lock:
                if self._state == "closed":
                    return
                self._state = "closing"
                try:
                    await self._shutdown()
                finally:
                    self._state = "closed"

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()


class _AdmissionContext:
    def __init__(
        self,
        manager: ResourceManager,
        requirements: WorkloadRequirements | dict[str, object],
        *,
        wait: bool,
        wait_timeout_seconds: float | None,
    ) -> None:
        self._manager = manager
        self._requirements = requirements
        self._wait = wait
        self._wait_timeout_seconds = wait_timeout_seconds
        self._lease: ResourceLease | None = None
        self._scope: anyio.CancelScope | None = None
        self._closed = False

    async def __aenter__(self) -> AdmissionDecision:
        if self._lease is not None or self._closed:
            raise ManagerStateError("an admission context cannot be reused")
        lease = await self._manager.reserve(
            self._requirements, wait=self._wait, wait_timeout_seconds=self._wait_timeout_seconds
        )
        self._lease = lease
        scope = anyio.CancelScope()
        self._scope = scope
        scope.__enter__()
        token = anyio.lowlevel.current_token()

        def lost() -> None:
            def cancel_if_active() -> None:
                if not self._closed:
                    scope.cancel()

            _schedule_in_loop(token, cancel_if_active)

        try:
            await self._manager._call_runtime(
                lambda runtime: runtime.coordinator().register_loss_callback(lease.id, lost)
            )
            await anyio.lowlevel.checkpoint_if_cancelled()
            return lease.decision
        except BaseException as exc:
            suppressed = scope.__exit__(type(exc), exc, exc.__traceback__)
            self._closed = True
            with anyio.CancelScope(shield=True):
                try:
                    await self._manager.release(lease)
                except (ManagerStateError, ReservationError):
                    _LOG.exception("Could not release a reservation during admission setup")
            if suppressed:
                raise ReservationError("reservation was lost during admission setup") from exc
            raise

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if self._lease is None or self._scope is None:
            raise ManagerStateError("admission context was not entered")
        suppressed = self._scope.__exit__(exc_type, exc, traceback)
        self._closed = True
        with anyio.CancelScope(shield=True):
            try:
                await self._manager.release(self._lease)
            except (ManagerStateError, ReservationError) as error:
                if exc_type is None or suppressed:
                    raise ReservationError(
                        "admission lease was lost or could not be released"
                    ) from (error)
                _LOG.exception("Could not release a reservation while propagating an error")
        return suppressed
