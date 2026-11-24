"""Bounded resource-aware admission and lease supervision."""

from __future__ import annotations

import logging
import re
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import anyio

from resourcera.config import ResourceConfig
from resourcera.coordination import ReservationStore
from resourcera.exceptions import AdmissionRejectedError, ManagerStateError, ReservationError
from resourcera.models import (
    AdmissionDecision,
    AdmissionState,
    AdmissionWaiterState,
    CapacityResult,
    ResourceLease,
    ResourceSnapshot,
    WorkloadRequirements,
)

_LOG = logging.getLogger(__name__)
_LEASE_ID = re.compile(r"[A-Za-z0-9._-]{1,128}\Z")


@dataclass(slots=True)
class _Owned:
    lease: ResourceLease
    auto_renew: bool
    deliveries: dict[str, threading.Event]
    delivered: bool = False
    on_loss: Callable[[], None] | None = None


_HEAD_POLL_SECONDS = 0.25
# Event-loop timers may fire up to one clock tick early (about 15.6 ms on Windows);
# heartbeats tolerate that instead of spinning. Deadlines never use this tolerance.
_CLOCK_TOLERANCE = time.get_clock_info("monotonic").resolution
# A queued waiter whose last heartbeat is older than this many intervals is reported as
# overdue. It is still queued and waiting; it is never removed or admitted because of it.
_OVERDUE_INTERVALS = 2


@dataclass(slots=True, eq=False)
class _Waiter:
    """One queued admission request; ``outcome`` leaves ``"waiting"`` exactly once."""

    priority: int
    sequence: int
    started: float
    lease_id: str = ""
    timeout: float | None = None
    deadline: float | None = None
    next_heartbeat: float = float("inf")
    bypassed: int = 0
    outcome: str = "waiting"
    heartbeats: int = 0
    last_heartbeat: float | None = None
    missed_heartbeats: int = 0
    max_heartbeat_delay: float | None = None


class AdmissionCoordinator:
    """Serialize local admission and supervise a bounded set of leases."""

    def __init__(
        self,
        config: ResourceConfig,
        snapshot: Callable[[], ResourceSnapshot],
        record_cpu: Callable[[float], None],
        allow_admission: Callable[[], bool],
    ) -> None:
        self.config = config
        self._snapshot = snapshot
        self._record_cpu = record_cpu
        self._allow_admission = allow_admission
        self._store = ReservationStore(config)
        self._owner = str(uuid4())
        self._lock = anyio.Lock()
        self._worker_limit = anyio.CapacityLimiter(1)
        self._limiter = anyio.CapacityLimiter(config.max_concurrency)
        self._owned: dict[str, _Owned] = {}
        self._lost: OrderedDict[str, str] = OrderedDict()
        self._waiters: list[_Waiter] = []
        self._sequence = 0
        self._changed = anyio.Event()
        self._closing = False
        self._rejected_count = 0
        self._timeout_count = 0
        self._cancelled_waiter_count = 0
        self._completed_wait_count = 0
        self._total_wait_seconds = 0.0
        self._admitted_waiter_count = 0
        self._shutdown_waiter_count = 0
        self._heartbeat_count = 0
        self._missed_heartbeat_count = 0
        self._max_heartbeat_delay: float | None = None
        self._last_heartbeat: float | None = None

    @property
    def retained_objects(self) -> tuple[object, ...]:
        """Objects counted in managed-state overhead."""
        return (self._owned, self._lost, self._waiters)

    async def _call[T](self, operation: Callable[[], T]) -> T:
        measured: list[float] = []

        def run() -> T:
            started = time.thread_time()
            try:
                return operation()
            finally:
                measured.append(time.thread_time() - started)

        try:
            return await anyio.to_thread.run_sync(run, limiter=self._worker_limit)
        finally:
            if measured:
                self._record_cpu(measured[0])

    async def start(self) -> None:
        """Initialize the selected store before accepting work."""
        await self._call(self._store.initialize)

    def _notify(self) -> None:
        previous = self._changed
        self._changed = anyio.Event()
        previous.set()

    def _remember_loss(self, lease_id: str, reason: str) -> None:
        owned = self._owned.pop(lease_id, None)
        if owned is None:
            return
        self._limiter.release_on_behalf_of(lease_id)
        self._lost[lease_id] = reason
        while len(self._lost) > max(128, self.config.max_concurrency * 2):
            self._lost.popitem(last=False)
        _LOG.error("Reservation %s lost: %s", lease_id, reason)
        if owned.on_loss is not None:
            try:
                owned.on_loss()
            except Exception:
                _LOG.exception("Could not notify the owner of lost reservation %s", lease_id)
        self._notify()

    async def capacity(self, requirements: WorkloadRequirements | None) -> CapacityResult:
        """Include all active reservations in the current capacity estimate."""
        snapshot = self._snapshot()
        return await self._call(lambda: self._store.capacity(snapshot, requirements))

    async def state(self) -> AdmissionState:
        """Snapshot this manager's queue and owned leases without sampling resources."""
        async with self._lock:
            return self._state_now()

    def _state_now(self) -> AdmissionState:
        """Build the admission view synchronously; it never yields, so it is consistent."""
        now = time.monotonic()
        wall = datetime.now(UTC)
        interval = self.config.admission_heartbeat_interval_seconds
        priorities: dict[int, int] = {}
        views: list[AdmissionWaiterState] = []
        for position, waiter in enumerate(self._waiters):
            priorities[waiter.priority] = priorities.get(waiter.priority, 0) + 1
            beat = waiter.last_heartbeat
            overdue = (
                interval is not None
                and now - (waiter.started if beat is None else beat) > interval * _OVERDUE_INTERVALS
            )
            views.append(
                AdmissionWaiterState(
                    sequence=waiter.sequence,
                    lease_id=waiter.lease_id,
                    priority=waiter.priority,
                    position=position,
                    status="heartbeat_overdue" if overdue else "waiting",
                    wait_seconds=max(0.0, now - waiter.started),
                    timeout_seconds=waiter.timeout,
                    remaining_seconds=(
                        None if waiter.deadline is None else max(0.0, waiter.deadline - now)
                    ),
                    heartbeat_count=waiter.heartbeats,
                    last_heartbeat_age_seconds=None if beat is None else max(0.0, now - beat),
                    missed_heartbeat_count=waiter.missed_heartbeats,
                    max_heartbeat_delay_seconds=waiter.max_heartbeat_delay,
                )
            )
        indefinite = sum(1 for waiter in self._waiters if waiter.deadline is None)
        last = self._last_heartbeat
        return AdmissionState(
            queue_capacity=self.config.max_waiters,
            queue_depth=len(self._waiters),
            available_queue_slots=self.config.max_waiters - len(self._waiters),
            oldest_wait_seconds=(
                max(0.0, now - min(waiter.started for waiter in self._waiters))
                if self._waiters
                else None
            ),
            priority_distribution=tuple(sorted(priorities.items(), reverse=True)),
            active_reservations=len(self._owned),
            rejected_count=self._rejected_count,
            timeout_count=self._timeout_count,
            cancelled_waiter_count=self._cancelled_waiter_count,
            completed_wait_count=self._completed_wait_count,
            total_wait_seconds=self._total_wait_seconds,
            admitted_waiter_count=self._admitted_waiter_count,
            shutdown_waiter_count=self._shutdown_waiter_count,
            indefinite_waiter_count=indefinite,
            finite_waiter_count=len(self._waiters) - indefinite,
            heartbeat_interval_seconds=interval,
            heartbeat_count=self._heartbeat_count,
            missed_heartbeat_count=self._missed_heartbeat_count,
            max_heartbeat_delay_seconds=self._max_heartbeat_delay,
            last_heartbeat_at=(
                None if last is None else wall - timedelta(seconds=max(0.0, now - last))
            ),
            heartbeat_overdue_waiter_count=sum(
                1 for view in views if view.status == "heartbeat_overdue"
            ),
            waiters=tuple(views),
        )

    def _heartbeat(self, waiter: _Waiter) -> None:
        """Record liveness for a still-queued waiter when its heartbeat is due.

        A heartbeat only updates counters and timestamps: it never touches the queue,
        permits, reservations, priorities or the deadline, and it cannot revive a waiter
        that already left the queue. Late heartbeats are coalesced into one and the
        skipped intervals are counted as missed rather than replayed.
        """
        interval = self.config.admission_heartbeat_interval_seconds
        now = time.monotonic()
        if (
            interval is None
            or waiter.outcome != "waiting"
            or now + _CLOCK_TOLERANCE < waiter.next_heartbeat
        ):
            return
        delay = max(0.0, now - waiter.next_heartbeat)
        missed = int(delay // interval)
        waiter.heartbeats += 1
        waiter.last_heartbeat = now
        waiter.missed_heartbeats += missed
        waiter.max_heartbeat_delay = max(delay, waiter.max_heartbeat_delay or 0.0)
        waiter.next_heartbeat = now + interval
        self._heartbeat_count += 1
        self._missed_heartbeat_count += missed
        self._max_heartbeat_delay = max(delay, self._max_heartbeat_delay or 0.0)
        self._last_heartbeat = now

    def _leave(self, waiter: _Waiter, outcome: str) -> None:
        """End a waiter's queue lifecycle exactly once; later calls are ignored."""
        if waiter.outcome != "waiting":
            return
        waiter.outcome = outcome
        if outcome == "admitted":
            self._admitted_waiter_count += 1
        elif outcome == "shutdown":
            self._shutdown_waiter_count += 1
        if waiter in self._waiters:
            self._waiters.remove(waiter)
            self._record_wait(waiter)
            self._notify()

    async def _park(self, waiter: _Waiter, changed: anyio.Event, poll: bool) -> None:
        """Sleep until the queue changes, the head poll or deadline is due, heartbeating.

        Heartbeat wake-ups only record liveness and go back to sleep; they never re-run
        admission and never move the caller's deadline.
        """
        interval = self.config.admission_heartbeat_interval_seconds
        poll_at = time.monotonic() + _HEAD_POLL_SECONDS if poll else None
        wake_at = min((at for at in (waiter.deadline, poll_at) if at is not None), default=None)
        while True:
            self._heartbeat(waiter)
            target = (
                wake_at
                if interval is None
                else min(waiter.next_heartbeat, float("inf") if wake_at is None else wake_at)
            )
            delay = None if target is None else max(0.0, target - time.monotonic())
            with anyio.move_on_after(delay):
                await changed.wait()
                return
            if wake_at is not None and time.monotonic() >= wake_at:
                return

    def _record_wait(self, waiter: _Waiter) -> None:
        self._completed_wait_count += 1
        self._total_wait_seconds += max(0.0, time.monotonic() - waiter.started)

    def _enqueue(self, waiter: _Waiter) -> None:
        """Insert behind equal or higher priorities and behind exhausted bypass budgets.

        The bypass budget is charged at insertion: every waiter a later, higher-priority
        request is inserted ahead of counts one bypass, even if that request is later
        cancelled or times out. A waiter charged ``admission_max_bypass`` times is pinned,
        so it is admitted after at most the waiters ahead of it at enqueue plus
        ``admission_max_bypass`` others. The scan is bounded by ``max_waiters``.
        """
        position = len(self._waiters)
        while position:
            ahead = self._waiters[position - 1]
            if (
                ahead.priority >= waiter.priority
                or ahead.bypassed >= self.config.admission_max_bypass
            ):
                break
            position -= 1
        for overtaken in self._waiters[position:]:
            overtaken.bypassed += 1
        self._waiters.insert(position, waiter)

    async def _rejected(self, requirements: WorkloadRequirements, reason: str) -> AdmissionDecision:
        capacity = await self.capacity(requirements)
        return AdmissionDecision(
            accepted=False,
            requirements=requirements,
            capacity=capacity,
            policy=self._store.policy_name,
            reason=reason,
        )

    async def _attempt(
        self,
        requirements: WorkloadRequirements,
        lease_id: str,
        auto_renew: bool,
        attempt_id: str,
        delivery: threading.Event,
    ) -> tuple[AdmissionDecision, ResourceLease | None]:
        if self._closing:
            raise ManagerStateError("the resource manager is closing")
        if lease_id in self._lost:
            raise ReservationError(f"reservation {lease_id} was lost: {self._lost[lease_id]}")
        owned = self._owned.get(lease_id)
        if owned is not None:
            if owned.lease.expires_monotonic <= time.monotonic():
                self._remember_loss(lease_id, "lease expired")
                raise ReservationError(f"reservation {lease_id} expired")
            if owned.lease.requirements != requirements or owned.auto_renew != auto_renew:
                raise ReservationError("lease ID is already in use with different requirements")
            for other, event in tuple(owned.deliveries.items()):
                if event.is_set():
                    owned.delivered = True
                    del owned.deliveries[other]
            owned.deliveries[attempt_id] = delivery
            return owned.lease.decision, owned.lease
        if not self._allow_admission():
            return (
                await self._rejected(
                    requirements, "Admission is suspended by the strict self-overhead policy."
                ),
                None,
            )

        self._limiter.total_tokens = self.config.max_concurrency
        try:
            self._limiter.acquire_on_behalf_of_nowait(lease_id)
        except anyio.WouldBlock:
            return (
                await self._rejected(
                    requirements, "The configured application concurrency ceiling is reached."
                ),
                None,
            )
        try:
            snapshot = self._snapshot()
            with anyio.CancelScope(shield=True):
                decision, lease, _created = await self._call(
                    lambda: self._store.acquire(snapshot, requirements, self._owner, lease_id)
                )
            if lease is None:
                self._limiter.release_on_behalf_of(lease_id)
                return decision, None
            self._owned[lease_id] = _Owned(lease, auto_renew, {attempt_id: delivery})
            extra = decision.capacity.max_concurrency or 0
            before = self._limiter.borrowed_tokens - 1
            self._limiter.total_tokens = max(1, min(self.config.max_concurrency, before + extra))
            return decision, lease
        except BaseException:
            if lease_id not in self._owned:
                self._limiter.release_on_behalf_of(lease_id)
            raise

    async def reserve(
        self,
        requirements: WorkloadRequirements,
        *,
        lease_id: str | None,
        wait: bool,
        wait_timeout_seconds: float | None,
        auto_renew: bool,
        attempt_id: str,
        delivery: threading.Event,
    ) -> ResourceLease:
        """Acquire atomically, optionally waiting in a bounded local priority queue."""
        lease_id = str(uuid4()) if lease_id is None else lease_id
        if not _LEASE_ID.fullmatch(lease_id):
            raise ValueError("lease_id must contain 1-128 ASCII letters, digits, '.', '_' or '-'")
        if wait_timeout_seconds is not None and not 0 < wait_timeout_seconds < float("inf"):
            raise ValueError("admission timeout must be finite and positive")
        deadline = None if wait_timeout_seconds is None else time.monotonic() + wait_timeout_seconds
        waiter: _Waiter | None = None
        lease: ResourceLease | None = None
        decision: AdmissionDecision | None = None
        timed_out = False
        try:
            if wait:
                if self._closing:
                    raise ManagerStateError("the resource manager is closing")
                if len(self._waiters) >= self.config.max_waiters:
                    raise AdmissionRejectedError(
                        await self._rejected(requirements, "The bounded admission queue is full.")
                    )
                self._sequence += 1
                started = time.monotonic()
                interval = self.config.admission_heartbeat_interval_seconds
                waiter = _Waiter(
                    requirements.priority,
                    self._sequence,
                    started,
                    lease_id=lease_id,
                    timeout=wait_timeout_seconds,
                    deadline=deadline,
                    next_heartbeat=float("inf") if interval is None else started + interval,
                )
                self._enqueue(waiter)
                self._notify()
            while True:
                if waiter is not None:
                    self._heartbeat(waiter)
                async with self._lock:
                    if self._closing:
                        raise ManagerStateError("the resource manager is closing")
                    if waiter is not None and deadline is not None and time.monotonic() >= deadline:
                        timed_out = True
                        if decision is None:
                            decision = await self._rejected(requirements, "Admission timed out.")
                        else:
                            decision = decision.model_copy(
                                update={"reason": f"Admission timed out: {decision.reason}"}
                            )
                        raise AdmissionRejectedError(decision, timed_out=True)
                    is_head = waiter is not None and self._waiters[0] is waiter
                    if waiter is None and self._waiters:
                        decision = await self._rejected(
                            requirements, "Earlier queued admission requests have priority."
                        )
                    elif waiter is None or is_head:
                        decision, lease = await self._attempt(
                            requirements, lease_id, auto_renew, attempt_id, delivery
                        )
                        if lease is not None:
                            if (
                                waiter is not None
                                and deadline is not None
                                and time.monotonic() >= deadline
                            ):
                                timed_out = True
                                decision = decision.model_copy(
                                    update={
                                        "accepted": False,
                                        "lease_id": None,
                                        "reason": (
                                            "Admission timed out before the lease was returned."
                                        ),
                                    }
                                )
                                raise AdmissionRejectedError(decision, timed_out=True)
                            if waiter is not None:
                                self._leave(waiter, "admitted")
                                waiter = None
                            break
                    changed = self._changed
                if not wait:
                    if decision is None:
                        raise RuntimeError("an immediate admission decision was not produced")
                    raise AdmissionRejectedError(decision)
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    timed_out = True
                    if decision is None:
                        decision = await self._rejected(requirements, "Admission timed out.")
                    else:
                        decision = decision.model_copy(
                            update={"reason": f"Admission timed out: {decision.reason}"}
                        )
                    raise AdmissionRejectedError(decision, timed_out=True)
                if waiter is None:
                    raise RuntimeError("a waiting admission request has no queue entry")
                await self._park(waiter, changed, is_head)
            await anyio.lowlevel.checkpoint_if_cancelled()
            if lease is None:
                raise RuntimeError("an accepted admission did not create a lease")
            return lease
        except BaseException as exc:
            outcome = "rejected"
            if isinstance(exc, AdmissionRejectedError):
                self._rejected_count += 1
                if timed_out:
                    self._timeout_count += 1
                    outcome = "timed_out"
            elif isinstance(exc, anyio.get_cancelled_exc_class()):
                outcome = "cancelled"
                if waiter is not None:
                    self._cancelled_waiter_count += 1
            elif isinstance(exc, ManagerStateError) and self._closing:
                outcome = "shutdown"
            if lease is not None:
                with anyio.CancelScope(shield=True):
                    try:
                        await self.compensate(lease.id, attempt_id)
                    except ReservationError:
                        _LOG.exception("Could not compensate a cancelled reservation")
            if waiter is not None:
                with anyio.CancelScope(shield=True):
                    async with self._lock:
                        self._leave(waiter, outcome)
            raise

    async def compensate(self, lease_id: str, attempt_id: str) -> None:
        """Release only a lease created by this cancelled acquisition attempt."""
        async with self._lock:
            owned = self._owned.get(lease_id)
            if owned is None:
                return
            delivery = owned.deliveries.pop(attempt_id, None)
            if delivery is None:
                return
            if delivery.is_set():
                owned.delivered = True
            for other, event in tuple(owned.deliveries.items()):
                if event.is_set():
                    owned.delivered = True
                    del owned.deliveries[other]
            if owned.delivered or owned.deliveries:
                return
            with anyio.CancelScope(shield=True):
                released = await self._call(lambda: self._store.release(self._owner, (lease_id,)))
            if lease_id not in released:
                self._remember_loss(lease_id, "lease disappeared during cancellation compensation")
                return
            self._owned.pop(lease_id)
            self._limiter.release_on_behalf_of(lease_id)
            self._notify()

    async def register_loss_callback(self, lease_id: str, callback: Callable[[], None]) -> None:
        """Register cancellation of an admitted workload if its lease is lost."""
        async with self._lock:
            owned = self._owned.get(lease_id)
            if owned is None or owned.lease.expires_monotonic <= time.monotonic():
                if owned is not None:
                    self._remember_loss(lease_id, "lease expired")
                raise ReservationError(f"reservation {lease_id} is no longer active")
            if owned.on_loss is not None:
                raise ReservationError("a lease cannot be used by multiple admission contexts")
            owned.on_loss = callback

    async def inspect(self, lease_id: str) -> ResourceLease:
        """Return the latest owned lease or report that it was lost."""
        async with self._lock:
            owned = self._owned.get(lease_id)
            if owned is not None and owned.lease.expires_monotonic <= time.monotonic():
                self._remember_loss(lease_id, "lease expired")
                owned = None
            if owned is None:
                raise ReservationError(self._lost.get(lease_id, "reservation is not active"))
            return owned.lease

    async def renew(self, lease_id: str) -> ResourceLease:
        """Renew a lease without reviving an expired or foreign reservation."""
        async with self._lock:
            owned = self._owned.get(lease_id)
            if owned is None:
                raise ReservationError(self._lost.get(lease_id, "reservation is not active"))
            with anyio.CancelScope(shield=True):
                renewed = await self._call(lambda: self._store.renew(self._owner, (lease_id,)))
            lease = renewed.get(lease_id)
            if lease is None:
                self._remember_loss(lease_id, "lease expired or disappeared before renewal")
                raise ReservationError(f"reservation {lease_id} was lost before renewal")
            owned.lease = lease
            return lease

    async def release(self, lease_id: str) -> None:
        """Release an owned lease idempotently; surface known lease loss."""
        async with self._lock:
            if lease_id in self._lost:
                raise ReservationError(f"reservation {lease_id} was lost: {self._lost[lease_id]}")
            owned = self._owned.get(lease_id)
            with anyio.CancelScope(shield=True):
                released = await self._call(lambda: self._store.release(self._owner, (lease_id,)))
            if owned is not None and lease_id not in released:
                self._remember_loss(lease_id, "lease expired or disappeared before release")
                raise ReservationError(f"reservation {lease_id} was lost before release")
            if owned is not None:
                self._owned.pop(lease_id)
                self._limiter.release_on_behalf_of(lease_id)
                self._notify()

    async def supervise(self) -> None:
        """Renew all active leases with one bounded background task."""
        interval = min(10.0, max(0.01, self.config.lease_duration_seconds / 3))
        while not self._closing:
            await anyio.sleep(interval)
            async with self._lock:
                now = time.monotonic()
                for lease_id, owned in tuple(self._owned.items()):
                    if owned.lease.expires_monotonic <= now:
                        self._remember_loss(lease_id, "lease expired")
                due = tuple(
                    lease_id
                    for lease_id, owned in self._owned.items()
                    if owned.auto_renew
                    and owned.lease.expires_monotonic - now
                    <= self.config.lease_duration_seconds * 2 / 3
                )
                if not due:
                    continue
                try:
                    with anyio.CancelScope(shield=True):
                        renewed = await self._call(
                            lambda lease_ids=due: self._store.renew(self._owner, lease_ids)
                        )
                except ReservationError as exc:
                    _LOG.warning("Could not renew reservations: %s", exc)
                    continue
                for lease_id in due:
                    lease = renewed.get(lease_id)
                    if lease is None:
                        self._remember_loss(lease_id, "lease disappeared before renewal")
                    elif lease_id in self._owned:
                        self._owned[lease_id].lease = lease

    async def close(self) -> None:
        """Stop admission, notify active contexts, and release owned leases."""
        async with self._lock:
            self._closing = True
            ids = tuple(self._owned)
            self._notify()
            try:
                if ids:
                    with anyio.CancelScope(shield=True):
                        await self._call(lambda: self._store.release(self._owner, ids))
            finally:
                for lease_id in ids:
                    self._remember_loss(lease_id, "resource manager closed")
