"""Resource-aware concurrency recommendations and adaptive slot control."""

from __future__ import annotations

import logging
import time
from collections import deque
from collections.abc import Callable
from concurrent.futures import Executor, Future
from datetime import UTC, datetime
from types import TracebackType
from typing import TYPE_CHECKING

import anyio
import anyio.lowlevel

from resourcera.exceptions import (
    AdmissionRejectedError,
    ManagerStateError,
    ProviderError,
    ReservationError,
)
from resourcera.models import (
    AdmissionDecision,
    CapacityResult,
    ConcurrencyAdjustment,
    ConcurrencyRecommendation,
    ConcurrencyState,
    MetricStatus,
    ResourceSnapshot,
    WorkloadRequirements,
)

if TYPE_CHECKING:
    from resourcera.manager import ResourceManager, _AdmissionContext

_LOG = logging.getLogger(__name__)
_USABLE = {MetricStatus.AVAILABLE, MetricStatus.ESTIMATED}
_POLICY = "adaptive-concurrency"


def _required(requirements: WorkloadRequirements) -> tuple[str, ...]:
    keys = [
        key
        for key, demand in (
            ("cpu", requirements.cpu),
            ("memory", requirements.memory),
            ("disk_read", requirements.disk_read_bytes_per_second),
            ("disk_write", requirements.disk_write_bytes_per_second),
            ("network", requirements.network_bytes_per_second),
        )
        if demand is not None
    ]
    if (
        requirements.accelerator_type is not None
        or requirements.accelerator_memory is not None
        or requirements.accelerator_count is not None
    ):
        keys.append("accelerators")
    return tuple(keys)


def _confidence(capacity: CapacityResult, keys: tuple[str, ...], degraded: bool) -> float:
    if capacity.max_concurrency is None:
        return 0.0
    scores = []
    for key in keys:
        availability = capacity.available[key]
        if availability.status not in _USABLE:
            return 0.0
        default = 1.0 if availability.status is MetricStatus.AVAILABLE else 0.5
        scores.append(default if availability.confidence is None else availability.confidence)
    score = min(scores, default=0.0)
    return score / 2 if degraded else score


def _pressure(snapshot: ResourceSnapshot, now: float) -> dict[str, float | None]:
    return {
        name: reading.value if reading.status_at(now) in _USABLE else None
        for name, reading in (
            ("cpu", snapshot.pressure.cpu_percent),
            ("memory", snapshot.pressure.memory_percent),
            ("io", snapshot.pressure.io_percent),
        )
    }


def _workload_notes(
    snapshot: ResourceSnapshot, requirements: WorkloadRequirements, now: float
) -> list[str]:
    notes = []
    if requirements.network_bytes_per_second is not None:
        notes.append(
            "Network bandwidth does not determine request concurrency by itself; latency, "
            "connection pools, and remote rate limits must be expressed through the maximum."
        )
        connections = snapshot.network_connections
        if connections.status_at(now) in _USABLE and connections.value is not None:
            notes.append(
                f"{connections.value} network connections were observed; they are reported, "
                "not converted into a limit."
            )
    if (
        requirements.disk_read_bytes_per_second is not None
        or requirements.disk_write_bytes_per_second is not None
    ):
        for label, reading, unit in (
            ("Disk queue depth", snapshot.disk.queue_depth, ""),
            ("Disk latency", snapshot.disk.latency_ms, " ms"),
        ):
            if reading.status_at(now) in _USABLE and reading.value is not None:
                notes.append(f"{label} is {reading.value:g}{unit}; it is reported, not modeled.")
            else:
                notes.append(f"{label} is not exposed here and was not estimated.")
    return notes


def recommend(
    capacity: CapacityResult,
    snapshot: ResourceSnapshot,
    requirements: WorkloadRequirements,
    *,
    active: int,
    minimum: int,
    maximum: int,
    now: float | None = None,
) -> ConcurrencyRecommendation:
    """Turn a capacity estimate for one workload into a bounded concurrency limit."""
    if now is None:
        now = time.monotonic()
    keys = _required(requirements)
    degraded = snapshot.overhead.degraded
    assumptions = [*capacity.assumptions, *_workload_notes(snapshot, requirements, now)]
    if degraded:
        assumptions.append("Monitoring detail is reduced by the overhead policy.")
    limiting = capacity.limiting_resource
    if capacity.max_concurrency is None:
        detail = capacity.available[limiting].reason if limiting in capacity.available else ""
        detail = detail.rstrip(".")
        estimated = None
        limit = minimum
        reason = (
            f"Capacity for {limiting or 'a required resource'} is unknown or stale"
            f"{': ' + detail if detail else ''}. The minimum is reported, and admission rejects "
            "work until the measurement is usable."
        )
    else:
        estimated = active + capacity.max_concurrency
        if estimated > maximum:
            limit = maximum
            limiting = "requested_maximum"
            reason = f"Capacity allows {estimated}; the requested maximum of {maximum} applies."
        elif estimated < minimum:
            limit = minimum
            reason = (
                f"{limiting} allows only {estimated}; the minimum of {minimum} is reported, "
                "and admission still rejects work that does not fit."
            )
        else:
            limit = estimated
            reason = f"{limiting} limits this workload to {estimated} concurrent operations."
    return ConcurrencyRecommendation(
        recommended_limit=limit,
        min_concurrency=minimum,
        max_concurrency=maximum,
        estimated_limit=estimated,
        capacity_known=estimated is not None,
        limiting_resource=limiting,
        capacity=capacity,
        snapshot_id=capacity.snapshot_id,
        snapshot_age_seconds=capacity.snapshot_age_seconds,
        safety_margin=capacity.safety_margin,
        assumptions=tuple(assumptions),
        reason=reason,
        confidence=_confidence(capacity, keys, degraded),
    )


def validate_bounds(minimum: int, maximum: int) -> None:
    """Reject concurrency bounds that cannot describe a usable limit."""
    if isinstance(minimum, bool) or isinstance(maximum, bool):
        raise TypeError("concurrency bounds must be integers")
    if minimum < 1 or maximum < minimum:
        raise ValueError("concurrency bounds require 1 <= min_concurrency <= max_concurrency")


def _now() -> datetime:
    return datetime.now(UTC)


class ConcurrencyController:
    """Bound one workload's concurrency with leased slots and a deterministic AIMD policy.

    Use it through ``ResourceManager.adaptive_concurrency``. Every slot holds a logical lease,
    so controllers for different workloads share the manager's reservation accounting.
    """

    def __init__(
        self,
        manager: ResourceManager,
        requirements: WorkloadRequirements,
        *,
        minimum: int,
        maximum: int,
        max_waiters: int,
        wait_timeout_seconds: float | None,
    ) -> None:
        config = manager.config
        self.requirements = requirements
        self._manager = manager
        self._minimum = minimum
        self._maximum = maximum
        self._max_waiters = max_waiters
        self._wait_timeout = wait_timeout_seconds
        self._interval = config.concurrency_adjustment_interval_seconds
        self._step = config.concurrency_increase_step
        self._history: deque[ConcurrencyAdjustment] = deque(maxlen=config.concurrency_history_size)
        self._limiter = anyio.CapacityLimiter(minimum)
        self._adjusting = anyio.Lock()
        self._waiting: set[anyio.CancelScope] = set()
        self._clock: Callable[[], float] = time.monotonic
        self._active = 0
        self._leased = 0
        self._streak = 0
        self._smoothed: float | None = None
        self._last_change: float | None = None
        self._last_adjusted_at: datetime | None = None
        self._last_evaluated_at: datetime | None = None
        self._recommendation: ConcurrencyRecommendation | None = None
        self._pressure: dict[str, float | None] = {}
        self._reason = "The controller has not evaluated capacity yet."
        self._calibrated = False
        self._closed = False

    @property
    def current_limit(self) -> int:
        """The number of slots new work may currently occupy."""
        return int(self._limiter.total_tokens)

    @property
    def state(self) -> ConcurrencyState:
        """Return an immutable view of the controller; no measurement is performed."""
        recommendation = self._recommendation
        return ConcurrencyState(
            current_limit=self.current_limit,
            recommended_limit=(
                self.current_limit if recommendation is None else recommendation.recommended_limit
            ),
            min_concurrency=self._minimum,
            max_concurrency=self._maximum,
            active_count=self._active,
            pending_count=len(self._waiting),
            available_permits=max(0, self.current_limit - self._limiter.borrowed_tokens),
            last_evaluated_at=self._last_evaluated_at,
            last_adjusted_at=self._last_adjusted_at,
            limiting_resource=None if recommendation is None else recommendation.limiting_resource,
            adjustment_reason=self._reason,
            capacity=None if recommendation is None else recommendation.capacity,
            confidence=0.0 if recommendation is None else recommendation.confidence,
            snapshot_age_seconds=(
                None if recommendation is None else recommendation.snapshot_age_seconds
            ),
            pressure_percent=dict(self._pressure),
            closed=self._closed,
            history=tuple(self._history),
        )

    def _set_limit(
        self, limit: int, reason: str, recommendation: ConcurrencyRecommendation
    ) -> None:
        previous = self.current_limit
        self._reason = reason
        if limit == previous:
            return
        self._limiter.total_tokens = limit
        self._last_change = self._clock()
        self._last_adjusted_at = _now()
        self._streak = 0
        self._history.append(
            ConcurrencyAdjustment(
                at=self._last_adjusted_at,
                previous_limit=previous,
                new_limit=limit,
                recommended_limit=recommendation.recommended_limit,
                limiting_resource=recommendation.limiting_resource,
                reason=reason,
            )
        )
        _LOG.info("Concurrency limit changed from %d to %d: %s", previous, limit, reason)

    async def adjust(self) -> ConcurrencyState:
        """Re-evaluate cached capacity and apply the adjustment policy once."""
        async with self._adjusting:
            if self._closed:
                raise ManagerStateError("the concurrency controller is closed")
            try:
                snapshot = await self._manager.snapshot()
                capacity = await self._manager.capacity(self.requirements)
            except ProviderError as exc:
                self._reason = f"Holding the limit because resource data failed: {exc}"
                return self.state
            now = self._clock()
            recommendation = recommend(
                capacity,
                snapshot,
                self.requirements,
                active=self._leased,
                minimum=self._minimum,
                maximum=self._maximum,
                now=time.monotonic(),
            )
            self._recommendation = recommendation
            self._pressure = _pressure(snapshot, time.monotonic())
            self._last_evaluated_at = _now()
            self._apply(
                recommendation,
                now,
                initial=not self._calibrated,
                degraded=snapshot.overhead.degraded,
            )
            return self.state

    def _apply(
        self,
        recommendation: ConcurrencyRecommendation,
        now: float,
        *,
        initial: bool,
        degraded: bool = False,
    ) -> None:
        current = self.current_limit
        target = recommendation.recommended_limit
        if initial:
            # Until capacity is first measurable the limit stays at the minimum; the first
            # usable estimate then sets the limit directly instead of probing upward.
            self._calibrated = recommendation.capacity_known
            self._smoothed = float(target)
            if self._last_change is None:
                label = "Initial limit"
            elif recommendation.capacity_known:
                label = "Calibrated"
            else:
                label = "Waiting for measurable capacity"
            self._set_limit(target, f"{label}: {recommendation.reason}", recommendation)
            self._last_change = now
            return
        if not recommendation.capacity_known:
            self._streak = 0
            self._reason = f"Holding at {current}: {recommendation.reason}"
            return
        if target < current:
            self._smoothed = float(target)
            self._set_limit(target, f"Decreased: {recommendation.reason}", recommendation)
            return
        self._smoothed = float(target) if self._smoothed is None else (self._smoothed + target) / 2
        if target == current or self._smoothed <= current:
            self._streak = 0
            self._reason = f"Holding at {current}: {recommendation.reason}"
            return
        if (
            degraded
            or recommendation.confidence <= 0
            or recommendation.capacity.snapshot_age_seconds
            > max(self._interval, self._manager.config.cpu_memory_interval_seconds) * 3
        ):
            self._streak = 0
            self._reason = (
                f"Holding at {current}: measurements are degraded or not fresh enough to increase."
            )
            return
        self._streak += 1
        elapsed = now - (now if self._last_change is None else self._last_change)
        if self._streak < 2 or elapsed < self._interval:
            self._reason = (
                f"Capacity allows {target}; waiting for a confirmed, rate-limited increase."
            )
            return
        self._set_limit(
            min(target, current + self._step),
            f"Increased additively: {recommendation.reason}",
            recommendation,
        )

    def signal_overload(self, reason: str = "The application reported overload.") -> None:
        """Halve the limit immediately after external feedback such as timeouts or HTTP 429."""
        if self._closed or self._recommendation is None:
            return
        limit = max(self._minimum, self.current_limit // 2)
        self._smoothed = float(limit)
        self._set_limit(limit, f"Decreased multiplicatively: {reason}", self._recommendation)
        self._last_change = self._clock()

    def _rejection(self, reason: str) -> AdmissionRejectedError:
        if self._recommendation is None:
            raise ManagerStateError("the concurrency controller was not started")
        return AdmissionRejectedError(
            AdmissionDecision(
                accepted=False,
                requirements=self.requirements,
                capacity=self._recommendation.capacity,
                policy=_POLICY,
                reason=reason,
            )
        )

    async def _acquire(self, token: object) -> None:
        if self._closed:
            raise ManagerStateError("the concurrency controller is closed")
        try:
            self._limiter.acquire_on_behalf_of_nowait(token)
            return
        except anyio.WouldBlock:
            pass
        if len(self._waiting) >= self._max_waiters:
            raise self._rejection("The controller's bounded waiter queue is full.")
        scope = anyio.CancelScope()
        self._waiting.add(scope)
        try:
            with scope:
                if self._wait_timeout is None:
                    await self._limiter.acquire_on_behalf_of(token)
                else:
                    with anyio.move_on_after(self._wait_timeout) as timeout:
                        await self._limiter.acquire_on_behalf_of(token)
                    if timeout.cancelled_caught:
                        raise self._rejection("Waiting for a concurrency slot timed out.")
        finally:
            self._waiting.discard(scope)
        if scope.cancelled_caught:
            raise ManagerStateError("the concurrency controller closed while waiting")

    def slot(self) -> ConcurrencySlot:
        """Return a single-use context that holds one permit and one logical lease."""
        return ConcurrencySlot(self)

    async def run_sync[T](
        self, executor: Executor, function: Callable[..., T], /, *args: object
    ) -> T:
        """Submit blocking work to an application-owned executor inside a slot.

        The slot is released only after the submitted callable has finished. If the caller is
        cancelled while the callable is running, cancellation is delivered after it completes.
        The executor is never shut down by this method.
        """
        async with self.slot():
            future = executor.submit(function, *args)
            return await _completion(future)

    def _close(self) -> None:
        self._closed = True
        for scope in tuple(self._waiting):
            scope.cancel()

    async def _run(self) -> None:
        while not self._closed:
            await anyio.sleep(self._interval)
            try:
                await self.adjust()
            except ManagerStateError:
                self._close()
                self._reason = "The resource manager stopped; the controller is closed."
                return
            except Exception:
                _LOG.exception("Concurrency adjustment failed; the limit is unchanged")


async def _completion[T](future: Future[T]) -> T:
    from resourcera.manager import _schedule_in_loop

    token = anyio.lowlevel.current_token()
    done = anyio.Event()

    def completed(_future: Future[T]) -> None:
        try:
            _schedule_in_loop(token, done.set)
        except RuntimeError:
            _LOG.debug("Event loop closed before executor completion", exc_info=True)

    future.add_done_callback(completed)
    try:
        await done.wait()
    except BaseException:
        future.cancel()
        with anyio.CancelScope(shield=True):
            await done.wait()
        raise
    return future.result()


class ConcurrencySlot:
    """One admitted operation: a controller permit plus a logical lease."""

    def __init__(self, controller: ConcurrencyController) -> None:
        self._controller = controller
        self._token: object | None = None
        self._admission: _AdmissionContext | None = None

    async def __aenter__(self) -> AdmissionDecision:
        if self._token is not None:
            raise ManagerStateError("a concurrency slot cannot be reused")
        controller = self._controller
        token = object()
        await controller._acquire(token)
        self._token = token
        try:
            admission = controller._manager.admission(
                controller.requirements, wait=True, wait_timeout_seconds=None
            )
            decision = await admission.__aenter__()
        except BaseException:
            controller._limiter.release_on_behalf_of(token)
            raise
        self._admission = admission
        controller._active += 1
        controller._leased += 1
        return decision

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._admission is None or self._token is None:
            raise ManagerStateError("the concurrency slot was not entered")
        controller = self._controller
        try:
            suppressed = await self._admission.__aexit__(exc_type, exc, traceback)
        finally:
            controller._active -= 1
            controller._leased -= 1
            controller._limiter.release_on_behalf_of(self._token)
        if suppressed and exc is not None:
            # Lease loss cancelled the body; surface it instead of silently ending the block.
            raise ReservationError("the slot's lease was lost while its work ran") from exc


class AdaptiveConcurrency:
    """Async context that owns one controller and its bounded adjustment task."""

    def __init__(self, controller: ConcurrencyController) -> None:
        self._controller = controller
        self._group: anyio.abc.TaskGroup | None = None

    async def __aenter__(self) -> ConcurrencyController:
        if self._group is not None:
            raise ManagerStateError("an adaptive concurrency context cannot be reused")
        await self._controller.adjust()
        group = anyio.create_task_group()
        await group.__aenter__()
        self._group = group
        group.start_soon(self._controller._run)
        return self._controller

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if self._group is None:
            raise ManagerStateError("the adaptive concurrency context was not entered")
        self._controller._close()
        self._group.cancel_scope.cancel()
        try:
            return await self._group.__aexit__(exc_type, exc, traceback)
        except BaseExceptionGroup as group:
            if exc is not None and len(group.exceptions) == 1 and group.exceptions[0] is exc:
                return False
            raise
