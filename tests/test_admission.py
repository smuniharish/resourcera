import asyncio
import sqlite3
import threading
import time
from collections.abc import Callable
from functools import partial
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import anyio
import anyio.lowlevel
import pytest
from hypothesis import given
from hypothesis import settings as settings_hypothesis
from hypothesis import strategies as st

from resourcera import (
    AdmissionRejectedError,
    AdmissionState,
    ManagerStateError,
    ReservationError,
    ResourceConfig,
    ResourceLease,
    ResourceManager,
    WorkloadRequirements,
)
from resourcera.admission import AdmissionCoordinator, _Waiter
from resourcera.coordination import ReservationStore
from resourcera.metrics import SampleClock, metric
from resourcera.models import LimitState, ResourceSnapshot, Scope


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


async def start_manager(snapshot: ResourceSnapshot, config: ResourceConfig) -> ResourceManager:
    manager = ResourceManager(config)
    await manager.start()
    assert manager._runtime is not None
    manager._runtime._snapshot = snapshot
    return manager


def settings(**values: object) -> ResourceConfig:
    return ResourceConfig.model_validate(
        {"cpu_memory_interval_seconds": 60, "cpu_budget_fraction": 1, **values}
    )


@pytest.mark.anyio
async def test_reservations_consume_capacity_and_are_idempotent(snapshot: ResourceSnapshot) -> None:
    manager = await start_manager(snapshot, settings(max_concurrency=3))
    request = WorkloadRequirements(cpu=0.5, memory="1GB")
    try:
        first = await manager.reserve(request, lease_id="first")
        same = await manager.reserve(request, lease_id="first")
        assert first == same
        assert first.scope is Scope.PROCESS
        with pytest.raises(ReservationError, match="different requirements"):
            await manager.reserve({"memory": "1GB"}, lease_id="first")
        second = await manager.reserve(request)
        capacity = await manager.capacity(request)
        assert capacity.max_concurrency == 0
        with pytest.raises(AdmissionRejectedError) as rejected:
            await manager.reserve(request)
        assert not rejected.value.decision.accepted
        assert rejected.value.decision.capacity.limiting_resource in {"cpu", "memory"}
        await manager.release(first)
        await manager.release(first)
        third = await manager.reserve(request)
        await manager.release(second)
        await manager.release(third)
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_process_managers_share_claims(snapshot: ResourceSnapshot) -> None:
    config = settings(max_concurrency=1)
    first = await start_manager(snapshot, config)
    second = await start_manager(snapshot, config)
    try:
        lease = await first.reserve({"memory": "1GB"})
        with pytest.raises(AdmissionRejectedError):
            await second.reserve({"memory": "1GB"})
        with pytest.raises(ReservationError, match="another owner"):
            await second.release(lease.id)
        await first.release(lease)
        replacement = await second.reserve({"memory": "1GB"})
        await second.release(replacement)
    finally:
        await second.close()
        await first.close()


@pytest.mark.anyio
async def test_memory_heavy_sessions_share_global_reservations(
    snapshot: ResourceSnapshot,
) -> None:
    memory = snapshot.memory.model_copy(
        update={
            "available_bytes": metric(
                10 * 1024**3, "bytes", "test", Scope.MACHINE, SampleClock.now(), 60
            ),
            "cgroup_limit_state": LimitState.UNLIMITED,
        }
    )
    manager = await start_manager(
        snapshot.model_copy(update={"memory": memory}),
        settings(max_concurrency=4, max_waiters=4, safety_margin=0),
    )
    session_limits = [anyio.CapacityLimiter(2), anyio.CapacityLimiter(2)]
    scopes: dict[tuple[int, int], anyio.CancelScope] = {}
    active: set[tuple[int, int]] = set()
    completed: set[tuple[int, int]] = set()
    finished_cancelled = anyio.Event()
    release = anyio.Event()
    victim: list[tuple[int, int]] = []

    async def process(session: int, index: int) -> None:
        key = (session, index)
        with anyio.CancelScope() as scope:
            scopes[key] = scope
            try:
                async with (
                    session_limits[session],
                    manager.admission({"memory": "3.2GiB"}, wait=True, wait_timeout_seconds=None),
                ):
                    active.add(key)
                    try:
                        await release.wait()
                        completed.add(key)
                    finally:
                        active.remove(key)
            finally:
                if victim and key == victim[0]:
                    finished_cancelled.set()

    try:
        with anyio.fail_after(10):
            async with anyio.create_task_group() as tasks:
                for session in range(2):
                    for index in range(2):
                        tasks.start_soon(process, session, index)
                while len(active) != 3 or (await manager.admission_state()).queue_depth != 1:  # noqa: ASYNC110
                    await anyio.sleep(0.01)
                assert all(sum(key[0] == session for key in active) <= 2 for session in range(2))
                victim.append(next(iter(active)))
                scopes[victim[0]].cancel()
                await finished_cancelled.wait()
                while len(active) != 3 or victim[0] in active:  # noqa: ASYNC110
                    await anyio.sleep(0.01)
                release.set()
        assert len(completed) == 3
        assert (await manager.admission_state()).active_reservations == 0
        with pytest.raises(RuntimeError, match="processing failed"):
            async with manager.admission({"memory": "3.2GiB"}):
                raise RuntimeError("processing failed")
        assert (await manager.admission_state()).active_reservations == 0
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_waiters_are_bounded_and_prioritized(snapshot: ResourceSnapshot) -> None:
    manager = await start_manager(snapshot, settings(max_concurrency=1, max_waiters=2))
    order: list[int] = []
    try:
        held = await manager.reserve({"memory": "1GB"})

        async def waiter(priority: int) -> None:
            lease = await manager.reserve(
                {"memory": "1GB", "priority": priority},
                wait=True,
                wait_timeout_seconds=3,
            )
            order.append(priority)
            await manager.release(lease)

        async def queue_size() -> int:
            async def size(runtime):
                return len(runtime.coordinator()._waiters)

            return await manager._call_runtime(size)

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(waiter, 1)
            with anyio.fail_after(2):
                while True:
                    if await queue_size() >= 1:
                        break
                    await anyio.sleep(0.01)
            tasks.start_soon(waiter, 10)
            with anyio.fail_after(2):
                while True:
                    if await queue_size() >= 2:
                        break
                    await anyio.sleep(0.01)
            state = await manager.admission_state()
            assert (state.queue_capacity, state.queue_depth, state.available_queue_slots) == (
                2,
                2,
                0,
            )
            assert state.priority_distribution == ((10, 1), (1, 1))
            assert state.oldest_wait_seconds is not None
            assert state.active_reservations == 1
            with pytest.raises(AdmissionRejectedError, match="queue is full"):
                await manager.reserve({"memory": "1GB"}, wait=True)
            with pytest.raises(AdmissionRejectedError, match="queued"):
                await manager.reserve({"memory": "1GB"})
            # Windows Python 3.12 monotonic ticks are ~15.6 ms; let the clock advance
            # past both enqueue times so the recorded wait is observable.
            queued_at = time.monotonic()
            while time.monotonic() <= queued_at:  # noqa: ASYNC110 - polls the clock itself
                await anyio.sleep(0.001)
            await manager.release(held)
        assert order == [10, 1]
        state = await manager.admission_state()
        assert state.queue_depth == 0
        assert state.completed_wait_count == 2
        assert state.total_wait_seconds > 0
        assert state.rejected_count == 2
    finally:
        await manager.close()


def test_bounded_bypass_orders_waiters_fifo_within_priority(snapshot: ResourceSnapshot) -> None:
    from resourcera.admission import AdmissionCoordinator, _Waiter

    def order(max_bypass: int, priorities: list[int]) -> list[int]:
        coordinator = AdmissionCoordinator(
            ResourceConfig(admission_max_bypass=max_bypass),
            lambda: snapshot,
            lambda _seconds: None,
            lambda: True,
        )
        for sequence, priority in enumerate(priorities):
            coordinator._enqueue(_Waiter(priority, sequence, 0.0))
        return [waiter.sequence for waiter in coordinator._waiters]

    # Equal priorities stay FIFO; higher priorities overtake within the bypass budget.
    assert order(8, [0, 0, 0]) == [0, 1, 2]
    assert order(8, [0, 5, 5, 9]) == [3, 1, 2, 0]
    # Once the low-priority waiter has been overtaken twice it is pinned.
    assert order(2, [0, 5, 5, 5, 5]) == [1, 2, 0, 3, 4]
    # Nothing overtakes a pinned waiter, even to reach lower priorities ahead of it.
    assert order(1, [0, 1, 9, 9]) == [1, 0, 2, 3]
    # Zero disables overtaking: plain arrival order.
    assert order(0, [0, 9, 5]) == [0, 1, 2]


@pytest.mark.anyio
async def test_low_priority_waiter_is_not_starved_by_later_high_priority_traffic(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(
        snapshot, settings(max_concurrency=1, max_waiters=8, admission_max_bypass=2)
    )
    admitted: list[str] = []
    active = 0
    peak = 0
    try:
        held = await manager.reserve({"memory": "1GB"})

        async def waiter(name: str, priority: int) -> None:
            nonlocal active, peak
            lease = await manager.reserve(
                {"memory": "1GB", "priority": priority}, wait=True, wait_timeout_seconds=None
            )
            active += 1
            peak = max(peak, active)
            admitted.append(name)
            await anyio.lowlevel.checkpoint()
            active -= 1
            await manager.release(lease)

        async def queued(depth: int) -> None:
            with anyio.fail_after(5):
                while (await manager.admission_state()).queue_depth != depth:
                    await anyio.lowlevel.checkpoint()

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(waiter, "low", 0)
            await queued(1)
            for index in range(4):
                tasks.start_soon(waiter, f"high-{index}", 10)
                await queued(2 + index)
            state = await manager.admission_state()
            assert state.priority_distribution == ((10, 4), (0, 1))
            await manager.release(held)
        assert admitted == ["high-0", "high-1", "low", "high-2", "high-3"]
        assert peak == 1
        state = await manager.admission_state()
        assert (state.queue_depth, state.active_reservations) == (0, 0)
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_bypass_budget_is_charged_at_insertion_even_if_the_overtaker_cancels(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(
        snapshot, settings(max_concurrency=1, max_waiters=4, admission_max_bypass=1)
    )
    admitted: list[str] = []

    async def queued(depth: int) -> None:
        with anyio.fail_after(5):
            while (await manager.admission_state()).queue_depth != depth:
                await anyio.lowlevel.checkpoint()

    async def waiter(name: str, priority: int, scope: anyio.CancelScope | None = None) -> None:
        with scope or anyio.CancelScope():
            lease = await manager.reserve(
                {"memory": "1GB", "priority": priority}, wait=True, wait_timeout_seconds=None
            )
            admitted.append(name)
            await manager.release(lease)

    try:
        held = await manager.reserve({"memory": "1GB"})
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(waiter, "low", 0)
            await queued(1)
            overtaker = anyio.CancelScope()
            tasks.start_soon(waiter, "cancelled", 9, overtaker)
            await queued(2)
            assert (await manager.admission_state()).priority_distribution == ((9, 1), (0, 1))
            overtaker.cancel()
            await queued(1)
            # The cancelled request already spent the low waiter's only bypass.
            tasks.start_soon(waiter, "high", 9)
            await queued(2)
            await manager.release(held)
        assert admitted == ["low", "high"]
        state = await manager.admission_state()
        assert (state.queue_depth, state.active_reservations) == (0, 0)
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_repeated_priority_contention_keeps_the_bypass_bound(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(
        snapshot, settings(max_concurrency=1, max_waiters=8, admission_max_bypass=1)
    )
    active = 0
    peak = 0

    async def queued(depth: int) -> None:
        with anyio.fail_after(5):
            while (await manager.admission_state()).queue_depth != depth:
                await anyio.lowlevel.checkpoint()

    try:
        for round_ in range(3):
            admitted: list[str] = []

            async def waiter(name: str, priority: int, into: list[str] = admitted) -> None:
                nonlocal active, peak
                lease = await manager.reserve(
                    {"memory": "1GB", "priority": priority}, wait=True, wait_timeout_seconds=None
                )
                active += 1
                peak = max(peak, active)
                into.append(name)
                await anyio.lowlevel.checkpoint()
                active -= 1
                await manager.release(lease)

            held = await manager.reserve({"memory": "1GB"})
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(waiter, "low", 0)
                await queued(1)
                for index in range(3):
                    tasks.start_soon(waiter, f"high-{index}", 5 + round_)
                    await queued(2 + index)
                await manager.release(held)
            assert admitted == ["high-0", "low", "high-1", "high-2"], round_
            state = await manager.admission_state()
            assert (state.queue_depth, state.active_reservations) == (0, 0)
        assert peak == 1
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_shutdown_with_mixed_priorities_rejects_every_waiter(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(
        snapshot, settings(max_concurrency=1, max_waiters=4, admission_max_bypass=1)
    )
    await manager.reserve({"memory": "1GB"})
    outcomes: list[type[BaseException]] = []

    async def waiter(priority: int) -> None:
        try:
            await manager.reserve(
                {"memory": "1GB", "priority": priority}, wait=True, wait_timeout_seconds=None
            )
        except ManagerStateError as error:
            outcomes.append(type(error))

    async with anyio.create_task_group() as tasks:
        for priority in (0, 9, 3, 9):
            tasks.start_soon(waiter, priority)
        with anyio.fail_after(5):
            while (await manager.admission_state()).queue_depth != 4:
                await anyio.lowlevel.checkpoint()
        with pytest.raises(AdmissionRejectedError, match="queue is full"):
            await manager.reserve({"memory": "1GB", "priority": 99}, wait=True)
        await manager.close()
    assert outcomes == [ManagerStateError] * 4


@pytest.mark.anyio
async def test_wait_timeout_and_external_cancellation_cleanup(snapshot: ResourceSnapshot) -> None:
    manager = await start_manager(snapshot, settings(max_concurrency=1))
    try:
        held = await manager.reserve({"memory": "1GB"})
        with pytest.raises(AdmissionRejectedError, match="timed out") as rejected:
            await manager.reserve({"memory": "1GB"}, wait=True, wait_timeout_seconds=0.15)
        assert rejected.value.decision.capacity.max_concurrency == 0
        assert rejected.value.timed_out
        state = await manager.admission_state()
        assert (state.rejected_count, state.timeout_count, state.completed_wait_count) == (1, 1, 1)
        finished = anyio.Event()
        scopes: list[anyio.CancelScope] = []

        async def waiting() -> None:
            with anyio.CancelScope() as scope:
                scopes.append(scope)
                try:
                    await manager.reserve({"memory": "1GB"}, wait=True, wait_timeout_seconds=3)
                finally:
                    finished.set()

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(waiting)
            with anyio.fail_after(2):
                while not scopes:
                    await anyio.lowlevel.checkpoint()
            scopes[0].cancel()
            with anyio.fail_after(2):
                await finished.wait()
        await manager.release(held)
        lease = await manager.reserve({"memory": "1GB"})
        await manager.release(lease)
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_explicit_none_waits_beyond_configured_admission_timeout(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(
        snapshot, settings(max_concurrency=1, admission_wait_timeout_seconds=0.05)
    )
    try:
        held = await manager.reserve({"memory": "1GB"})
        with pytest.raises(AdmissionRejectedError, match="timed out"):
            await manager.reserve({"memory": "1GB"}, wait=True)

        finished = anyio.Event()

        async def waiter() -> None:
            async with manager.admission({"memory": "1GB"}, wait=True, wait_timeout_seconds=None):
                finished.set()

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(waiter)
            await anyio.sleep(0.15)
            assert not finished.is_set()
            await manager.release(held)
            with anyio.fail_after(2):
                await finished.wait()
        replacement = await manager.reserve({"memory": "1GB"})
        await manager.release(replacement)
    finally:
        await manager.close()


@pytest.mark.anyio
@pytest.mark.parametrize("count", [200, 500, 1000])
async def test_bounded_admission_queue_under_contention(
    snapshot: ResourceSnapshot, count: int
) -> None:
    manager = await start_manager(
        snapshot,
        settings(max_concurrency=8, max_waiters=count, admission_wait_timeout_seconds=None),
    )
    completed = 0
    active = 0
    peak = 0
    try:

        async def worker() -> None:
            nonlocal completed, active, peak
            async with manager.admission({"memory": "1MB"}, wait=True):
                active += 1
                peak = max(peak, active)
                await anyio.lowlevel.checkpoint()
                active -= 1
                completed += 1

        with anyio.fail_after(90):
            async with anyio.create_task_group() as tasks:
                for _ in range(count):
                    tasks.start_soon(worker)
        state = await manager.admission_state()
        assert completed == count
        assert peak <= 8
        assert (state.queue_depth, state.active_reservations, state.rejected_count) == (0, 0, 0)
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_cancellation_after_commit_compensates(
    snapshot: ResourceSnapshot, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = await start_manager(snapshot, settings(max_concurrency=1))
    assert manager._runtime is not None
    store = manager._runtime.coordinator()._store
    acquire = store.acquire
    committed = threading.Event()
    proceed = threading.Event()

    def paused(*args):
        result = acquire(*args)
        committed.set()
        if not proceed.wait(3):
            raise RuntimeError("test transaction did not resume")
        return result

    monkeypatch.setattr(store, "acquire", paused)
    finished = anyio.Event()
    scopes: list[anyio.CancelScope] = []

    async def request() -> None:
        with anyio.CancelScope() as scope:
            scopes.append(scope)
            try:
                await manager.reserve({"memory": "1GB"})
            finally:
                finished.set()

    try:
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(request)
            with anyio.fail_after(3):
                assert await anyio.to_thread.run_sync(committed.wait, 2)
            scopes[0].cancel()
            proceed.set()
            with anyio.fail_after(3):
                await finished.wait()
        monkeypatch.setattr(store, "acquire", acquire)
        capacity = await manager.capacity({"memory": "1GB"})
        assert capacity.max_concurrency == 1
        replacement = await manager.reserve({"memory": "1GB"})
        await manager.release(replacement)
    finally:
        proceed.set()
        await manager.close()


@pytest.mark.anyio
async def test_cancelled_duplicate_does_not_revoke_delivered_lease(
    snapshot: ResourceSnapshot, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = await start_manager(snapshot, settings(max_concurrency=1))
    original_call = manager._call_runtime
    ready = anyio.Event()
    finished = anyio.Event()
    scopes: list[anyio.CancelScope] = []
    first = True

    async def delay_first(operation):
        nonlocal first
        result = await original_call(operation)
        if isinstance(result, ResourceLease) and result.id == "shared" and first:
            first = False
            ready.set()
            await anyio.sleep_forever()
        return result

    monkeypatch.setattr(manager, "_call_runtime", delay_first)

    async def interrupted() -> None:
        with anyio.CancelScope() as scope:
            scopes.append(scope)
            try:
                await manager.reserve({"memory": "1GB"}, lease_id="shared")
            finally:
                finished.set()

    try:
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(interrupted)
            with anyio.fail_after(2):
                await ready.wait()
            delivered = await manager.reserve({"memory": "1GB"}, lease_id="shared")
            scopes[0].cancel()
            with anyio.fail_after(2):
                await finished.wait()
        assert (await manager.reservation(delivered)).id == delivered.id
        await manager.release(delivered)
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_shutdown_during_commit_never_delivers_the_lease(
    snapshot: ResourceSnapshot, monkeypatch: pytest.MonkeyPatch
) -> None:
    from resourcera.coordination import _PROCESS_RECORDS

    manager = await start_manager(snapshot, settings(max_concurrency=1))
    assert manager._runtime is not None
    store = manager._runtime.coordinator()._store
    acquire = store.acquire
    committed = threading.Event()
    proceed = threading.Event()

    def paused(*args):
        result = acquire(*args)
        committed.set()
        if not proceed.wait(3):
            raise RuntimeError("test transaction did not resume")
        return result

    monkeypatch.setattr(store, "acquire", paused)
    outcome: list[object] = []

    async def request() -> None:
        try:
            outcome.append(await manager.reserve({"memory": "1GB"}, lease_id="raced"))
        except ManagerStateError as exc:
            outcome.append(exc)

    async def shutdown() -> None:
        await manager.close()

    try:
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(request)
            with anyio.fail_after(3):
                assert await anyio.to_thread.run_sync(committed.wait, 2)
            tasks.start_soon(shutdown)
            with anyio.fail_after(3):
                while manager._state == "running":
                    await anyio.lowlevel.checkpoint()
            proceed.set()
        assert len(outcome) == 1
        assert isinstance(outcome[0], ManagerStateError)
        assert "raced" not in _PROCESS_RECORDS
        assert manager._state == "closed"
    finally:
        proceed.set()
        await manager.close()


@pytest.mark.anyio
async def test_concurrent_and_repeated_shutdown_wake_queued_waiters(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(snapshot, settings(max_concurrency=1, max_waiters=4))
    held = await manager.reserve({"memory": "1GB"})
    errors: list[BaseException] = []

    async def waiting() -> None:
        try:
            await manager.reserve({"memory": "1GB"}, wait=True, wait_timeout_seconds=None)
        except ManagerStateError as exc:
            errors.append(exc)

    async with anyio.create_task_group() as tasks:
        for _ in range(3):
            tasks.start_soon(waiting)
        with anyio.fail_after(3):
            while (await manager.admission_state()).queue_depth != 3:
                await anyio.lowlevel.checkpoint()
        for _ in range(3):
            tasks.start_soon(manager.close)
    assert len(errors) == 3
    assert manager._state == "closed"
    await manager.close()
    with pytest.raises(ManagerStateError):
        await manager.reserve({"memory": "1GB"})
    with pytest.raises(ManagerStateError):
        await manager.release(held)
    with pytest.raises(ManagerStateError):
        await manager.start()


@pytest.mark.anyio
async def test_repeated_acquire_release_cancel_cycles_do_not_leak(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(snapshot, settings(max_concurrency=2, max_waiters=64))
    try:
        for round_number in range(20):
            async with anyio.create_task_group() as tasks:

                async def worker(index: int) -> None:
                    with anyio.CancelScope() as scope:
                        if index % 3 == 0:
                            scope.cancel()
                        async with manager.admission(
                            {"memory": "1GB"}, wait=True, wait_timeout_seconds=None
                        ):
                            await anyio.lowlevel.checkpoint()

                for index in range(12):
                    tasks.start_soon(worker, index + round_number)
            state = await manager.admission_state()
            assert (state.queue_depth, state.active_reservations) == (0, 0)
        assert (await manager.capacity({"memory": "1GB"})).max_concurrency == 2
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_wait_deadline_compensates_a_late_commit(
    snapshot: ResourceSnapshot, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = await start_manager(snapshot, settings(max_concurrency=1))
    assert manager._runtime is not None
    store = manager._runtime.coordinator()._store
    acquire = store.acquire

    def slow(*args):
        time.sleep(0.2)
        return acquire(*args)

    monkeypatch.setattr(store, "acquire", slow)
    try:
        with pytest.raises(AdmissionRejectedError, match="timed out"):
            await manager.reserve({"memory": "1GB"}, wait=True, wait_timeout_seconds=0.1)
        assert (await manager.capacity({"memory": "1GB"})).max_concurrency == 1
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_renewal_expiry_and_admission_loss_notification(
    snapshot: ResourceSnapshot, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = await start_manager(snapshot, settings(max_concurrency=1, lease_duration_seconds=0.6))
    try:
        lease = await manager.reserve({"memory": "1GB"})
        await anyio.sleep(0.8)
        renewed = await manager.reservation(lease)
        assert renewed.expires_monotonic > lease.expires_monotonic
        await manager.release(renewed)

        manual = await manager.reserve({"memory": "1GB"}, auto_renew=False)
        await anyio.sleep(0.8)
        with pytest.raises(ReservationError):
            await manager.reservation(manual)
        with pytest.raises(ReservationError):
            await manager.release(manual)

        assert manager._runtime is not None
        store = manager._runtime.coordinator()._store

        def lose(owner_id: str, lease_ids: tuple[str, ...]) -> dict[str, ResourceLease]:
            assert store.release(owner_id, lease_ids) == frozenset(lease_ids)
            return {}

        monkeypatch.setattr(store, "renew", lose)
        with pytest.raises(ReservationError, match="lease was lost"):
            with anyio.fail_after(2):
                async with manager.admission({"memory": "1GB"}):
                    await anyio.sleep_forever()
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_context_releases_on_error_and_rejects_reuse(snapshot: ResourceSnapshot) -> None:
    manager = await start_manager(snapshot, settings(max_concurrency=1))
    context = manager.admission({"memory": "1GB"})
    try:
        with pytest.raises(ValueError, match="workload error"):
            async with context as decision:
                assert decision.accepted
                raise ValueError("workload error")
        with pytest.raises(ManagerStateError, match="cannot be reused"):
            async with context:
                pass
        lease = await manager.reserve({"memory": "1GB"})
        await manager.release(lease)
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_sqlite_contention_does_not_block_event_loop(
    snapshot: ResourceSnapshot, tmp_path
) -> None:
    path = tmp_path / "coordination.sqlite"
    manager = await start_manager(snapshot, settings(coordination_database=path))
    connection = sqlite3.connect(path, isolation_level=None)
    ticked = anyio.Event()
    try:
        connection.execute("BEGIN EXCLUSIVE")

        async def tick() -> None:
            await anyio.sleep(0.02)
            ticked.set()

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(tick)
            with pytest.raises(ReservationError, match="SQLite coordination failed"):
                await manager.reserve({"memory": "1GB"})
            assert ticked.is_set()
        connection.execute("ROLLBACK")
        lease = await manager.reserve({"memory": "1GB"})
        assert lease.scope is Scope.MACHINE
        await manager.release(lease)
    finally:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        connection.close()
        await manager.close()


def test_store_requires_a_real_absolute_database_path() -> None:
    with pytest.raises(ValueError, match="absolute local file"):
        ResourceConfig(coordination_database=":memory:")
    with pytest.raises(ValueError, match="absolute local file"):
        ResourceConfig(coordination_database="relative.sqlite")
    assert ReservationStore(settings()).scope is Scope.PROCESS


async def _state(manager: ResourceManager) -> AdmissionState:
    return await manager.admission_state()


async def _until(
    manager: ResourceManager, predicate: Callable[[AdmissionState], bool], seconds: float = 10
) -> AdmissionState:
    """Poll local admission state until ``predicate`` holds; never a fixed sleep window."""
    with anyio.fail_after(seconds):
        while True:
            state = await manager.admission_state()
            if predicate(state):
                return state
            await anyio.sleep(0.01)


async def _coordinator(manager: ResourceManager) -> AdmissionCoordinator:
    async def get(runtime: Any) -> AdmissionCoordinator:
        return runtime.coordinator()

    return await manager._call_runtime(get)


def _beats(state: AdmissionState) -> int:
    return state.waiters[0].heartbeat_count if state.waiters else -1


@pytest.mark.anyio
async def test_indefinite_waiter_heartbeats_without_admission_until_resources_free(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(
        snapshot,
        settings(
            max_concurrency=1,
            admission_wait_timeout_seconds=0.05,
            admission_heartbeat_interval_seconds=0.05,
        ),
    )
    admitted = anyio.Event()
    try:
        held = await manager.reserve({"memory": "1GB"})

        async def waiter() -> None:
            async with manager.admission({"memory": "1GB"}, wait=True, wait_timeout_seconds=None):
                admitted.set()

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(waiter)
            # Many intervals and far beyond the 0.05 s configured admission timeout.
            state = await _until(manager, lambda state: _beats(state) >= 5)
            assert not admitted.is_set()
            (view,) = state.waiters
            assert view.timeout_seconds is None and view.remaining_seconds is None
            assert view.lease_id and view.sequence >= 1 and view.position == 0
            assert (state.indefinite_waiter_count, state.finite_waiter_count) == (1, 0)
            assert state.heartbeat_interval_seconds == 0.05
            assert state.heartbeat_count >= 5 and state.last_heartbeat_at is not None
            # Heartbeats are liveness only: nothing was admitted or reserved.
            assert (state.active_reservations, state.admitted_waiter_count) == (1, 0)
            assert state.timeout_count == 0
            await manager.release(held)
            with anyio.fail_after(5):
                await admitted.wait()
        state = await _state(manager)
        assert (state.queue_depth, state.admitted_waiter_count, state.active_reservations) == (
            0,
            1,
            0,
        )
        assert state.waiters == ()
        beats = state.heartbeat_count
        await anyio.sleep(0.2)  # several intervals: a leftover heartbeat would show here
        assert (await _state(manager)).heartbeat_count == beats
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_heartbeats_never_extend_a_finite_deadline(snapshot: ResourceSnapshot) -> None:
    manager = await start_manager(
        snapshot, settings(max_concurrency=1, admission_heartbeat_interval_seconds=0.05)
    )
    remaining: list[float] = []
    outcome: list[float] = []
    try:
        held = await manager.reserve({"memory": "1GB"})

        async def waiter() -> None:
            started = time.monotonic()
            with pytest.raises(AdmissionRejectedError, match="timed out") as rejected:
                await manager.reserve({"memory": "1GB"}, wait=True, wait_timeout_seconds=0.6)
            assert rejected.value.timed_out
            outcome.append(time.monotonic() - started)

        # A heartbeat that reset the deadline would push it out forever; fail_after bounds it.
        with anyio.fail_after(5):
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(waiter)
                while not outcome:
                    state = await _state(manager)
                    for view in state.waiters:
                        assert view.timeout_seconds == 0.6
                        assert view.remaining_seconds is not None
                        remaining.append(view.remaining_seconds)
                    await anyio.sleep(0.01)
        (elapsed,) = outcome
        assert elapsed >= 0.6 - 0.02  # one coarse Windows clock tick
        assert remaining == sorted(remaining, reverse=True)
        state = await _state(manager)
        assert state.heartbeat_count >= 3
        assert (state.timeout_count, state.queue_depth, state.admitted_waiter_count) == (1, 0, 0)
        await manager.release(held)
        assert (await _state(manager)).active_reservations == 0
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_omitted_and_none_timeouts_are_reported_and_cancellation_ends_heartbeats(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(
        snapshot,
        settings(
            max_concurrency=1,
            admission_wait_timeout_seconds=30,
            admission_heartbeat_interval_seconds=0.05,
        ),
    )
    try:
        held = await manager.reserve({"memory": "1GB"})
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(partial(manager.reserve, {"memory": "1GB"}, wait=True))
            tasks.start_soon(
                partial(manager.reserve, {"memory": "1GB"}, wait=True, wait_timeout_seconds=None)
            )
            state = await _until(
                manager,
                lambda state: (
                    state.queue_depth == 2 and all(view.heartbeat_count for view in state.waiters)
                ),
            )
            assert (state.indefinite_waiter_count, state.finite_waiter_count) == (1, 1)
            timeouts = sorted(view.timeout_seconds or 0.0 for view in state.waiters)
            assert timeouts == [0.0, 30.0]
            tasks.cancel_scope.cancel()
        state = await _state(manager)
        assert (state.queue_depth, state.cancelled_waiter_count) == (0, 2)
        assert (state.indefinite_waiter_count, state.finite_waiter_count) == (0, 0)
        beats = state.heartbeat_count
        await anyio.sleep(0.2)
        assert (await _state(manager)).heartbeat_count == beats
        await manager.release(held)
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_manager_shutdown_ends_waiter_heartbeats(snapshot: ResourceSnapshot) -> None:
    manager = await start_manager(
        snapshot, settings(max_concurrency=1, admission_heartbeat_interval_seconds=0.05)
    )
    errors: list[BaseException] = []
    try:
        await manager.reserve({"memory": "1GB"})
        coordinator = await _coordinator(manager)

        async def waiter() -> None:
            try:
                await manager.reserve({"memory": "1GB"}, wait=True, wait_timeout_seconds=None)
            except ManagerStateError as exc:
                errors.append(exc)

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(waiter)
            await _until(manager, lambda state: _beats(state) >= 2)
            await manager.close()
    finally:
        await manager.close()
    assert len(errors) == 1
    state = coordinator._state_now()
    assert (state.queue_depth, state.shutdown_waiter_count, state.admitted_waiter_count) == (
        0,
        1,
        0,
    )
    beats = state.heartbeat_count
    await anyio.sleep(0.2)
    assert coordinator._state_now().heartbeat_count == beats


@pytest.mark.anyio
async def test_congested_event_loop_reports_late_heartbeats_without_expiring_waiters(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(
        snapshot, settings(max_concurrency=1, admission_heartbeat_interval_seconds=0.05)
    )
    admitted = anyio.Event()
    try:
        held = await manager.reserve({"memory": "1GB"})

        async def waiter() -> None:
            async with manager.admission({"memory": "1GB"}, wait=True, wait_timeout_seconds=None):
                admitted.set()

        async def stall(runtime: Any) -> AdmissionState:
            time.sleep(0.5)  # noqa: ASYNC251 - deliberately congests the owning event loop
            return runtime.coordinator()._state_now()

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(waiter)
            await _until(manager, lambda state: _beats(state) >= 1)
            stalled = await manager._call_runtime(stall)
            (view,) = stalled.waiters
            assert view.status == "heartbeat_overdue"
            assert stalled.heartbeat_overdue_waiter_count == 1
            # Overdue is reported, but the waiter is still queued, not removed or expired.
            assert (stalled.queue_depth, stalled.timeout_count) == (1, 0)
            state = await _until(manager, lambda state: state.missed_heartbeat_count >= 1)
            (view,) = state.waiters
            assert view.missed_heartbeat_count >= 1
            assert view.max_heartbeat_delay_seconds is not None
            assert view.max_heartbeat_delay_seconds >= 0.3
            assert state.max_heartbeat_delay_seconds == view.max_heartbeat_delay_seconds
            await _until(manager, lambda state: state.heartbeat_overdue_waiter_count == 0)
            assert not admitted.is_set()
            await manager.release(held)
            with anyio.fail_after(5):
                await admitted.wait()
        state = await _state(manager)
        assert (state.admitted_waiter_count, state.timeout_count, state.rejected_count) == (1, 0, 0)
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_heartbeats_do_not_sample_and_managers_keep_isolated_state(
    snapshot: ResourceSnapshot,
) -> None:
    config = settings(max_concurrency=1, admission_heartbeat_interval_seconds=0.05)
    first = await start_manager(snapshot, config)
    second = await start_manager(snapshot, config)
    samples = 0
    try:
        held = await first.reserve({"memory": "1GB"})

        async def count_samples(runtime: Any) -> None:
            original = runtime._sample_blocking

            def sample(sample_io: bool) -> Any:
                nonlocal samples
                samples += 1
                return original(sample_io)

            runtime._sample_blocking = sample

        await first._call_runtime(count_samples)
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(
                partial(first.reserve, {"memory": "1GB"}, wait=True, wait_timeout_seconds=None)
            )
            await _until(first, lambda state: _beats(state) >= 5)
            assert samples == 0  # cpu_memory_interval_seconds=60: heartbeats never sample
            other = await _state(second)
            assert (other.heartbeat_count, other.queue_depth, other.last_heartbeat_at) == (
                0,
                0,
                None,
            )
            # Read the owner's state from a worker thread through the supported marshaling.
            from_thread = await anyio.to_thread.run_sync(
                anyio.from_thread.run, first.admission_state
            )
            assert from_thread.queue_depth == 1
            tasks.cancel_scope.cancel()
        await first.release(held)
    finally:
        await first.close()
        await second.close()


@pytest.mark.anyio
async def test_heartbeats_can_be_disabled_without_fabricated_values(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(
        snapshot, settings(max_concurrency=1, admission_heartbeat_interval_seconds=None)
    )
    try:
        held = await manager.reserve({"memory": "1GB"})
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(
                partial(manager.reserve, {"memory": "1GB"}, wait=True, wait_timeout_seconds=None)
            )
            await _until(manager, lambda state: state.queue_depth == 1)
            await anyio.sleep(0.3)  # longer than the head poll: still nothing fabricated
            state = await _state(manager)
            (view,) = state.waiters
            assert state.heartbeat_interval_seconds is None
            assert (state.heartbeat_count, state.last_heartbeat_at) == (0, None)
            assert (view.status, view.heartbeat_count, view.last_heartbeat_age_seconds) == (
                "waiting",
                0,
                None,
            )
            tasks.cancel_scope.cancel()
        await manager.release(held)
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_many_indefinite_waiters_share_their_own_tasks_and_clean_up(
    snapshot: ResourceSnapshot, anyio_backend: str
) -> None:
    count = 300
    interval = 0.05
    manager = await start_manager(
        snapshot,
        settings(
            max_concurrency=1,
            max_waiters=count,
            admission_heartbeat_interval_seconds=interval,
        ),
    )
    try:
        held = await manager.reserve({"memory": "1GB"})
        coordinator = await _coordinator(manager)
        baseline = len(asyncio.all_tasks()) if anyio_backend == "asyncio" else 0
        async with anyio.create_task_group() as tasks:
            for _ in range(count):
                tasks.start_soon(
                    partial(
                        manager.reserve, {"memory": "1MB"}, wait=True, wait_timeout_seconds=None
                    )
                )
            started = time.monotonic()
            state = await _until(
                manager,
                lambda state: (
                    state.queue_depth == count
                    and all(view.heartbeat_count >= 2 for view in state.waiters)
                ),
                seconds=30,
            )
            elapsed = time.monotonic() - started
            if anyio_backend == "asyncio":
                # Heartbeats run in each waiter's own task: no extra tasks per waiter.
                assert len(asyncio.all_tasks()) == baseline + count
            # Bounded: per-waiter views mirror the queue and keep no history.
            assert len(state.waiters) == state.queue_depth == count
            assert not hasattr(coordinator._waiters[0], "__dict__")
            # At most one coalesced heartbeat per waiter per elapsed interval.
            assert state.heartbeat_count <= count * (elapsed / interval + 2)
            assert state.indefinite_waiter_count == count
            tasks.cancel_scope.cancel()
        state = await _state(manager)
        assert (state.queue_depth, state.cancelled_waiter_count, state.waiters) == (0, count, ())
        assert state.active_reservations == 1
        beats = state.heartbeat_count
        await anyio.sleep(0.2)
        assert (await _state(manager)).heartbeat_count == beats
        await manager.release(held)
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_cancellation_racing_admission_of_a_heartbeating_waiter_does_not_leak(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(
        snapshot, settings(max_concurrency=1, admission_heartbeat_interval_seconds=0.05)
    )
    try:
        for _ in range(10):
            held = await manager.reserve({"memory": "1GB"})
            delivered: list[ResourceLease] = []

            async def waiter(delivered: list[ResourceLease] = delivered) -> None:
                delivered.append(
                    await manager.reserve({"memory": "1GB"}, wait=True, wait_timeout_seconds=None)
                )

            async with anyio.create_task_group() as tasks:
                tasks.start_soon(waiter)
                await _until(manager, lambda state: _beats(state) >= 1)
                await manager.release(held)
                tasks.cancel_scope.cancel()
            state = await _state(manager)
            # The waiter left exactly once. Either the cancellation won, or the lease was
            # delivered to the caller (and is still owned by it); never an orphaned lease.
            assert state.queue_depth == 0
            assert state.active_reservations == len(delivered)
            for lease in delivered:
                await manager.release(lease)
        state = await _state(manager)
        assert state.admitted_waiter_count + state.cancelled_waiter_count == 10
        assert state.active_reservations == 0
        lease = await manager.reserve({"memory": "1GB"})
        await manager.release(lease)
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_repeated_manager_cycles_with_heartbeating_waiters_leave_no_tasks_or_threads(
    snapshot: ResourceSnapshot, anyio_backend: str
) -> None:
    def threads() -> set[threading.Thread]:
        # AnyIO's cached worker threads belong to the running loop and are reused.
        return {
            thread
            for thread in threading.enumerate()
            if not thread.daemon and thread.name != "AnyIO worker thread"
        }

    before_threads = threads()
    before_tasks = len(asyncio.all_tasks()) if anyio_backend == "asyncio" else 0
    for _ in range(3):
        manager = await start_manager(
            snapshot, settings(max_concurrency=1, admission_heartbeat_interval_seconds=0.05)
        )
        try:
            await manager.reserve({"memory": "1GB"})
            async with anyio.create_task_group() as tasks:

                async def waiter(manager: ResourceManager = manager) -> None:
                    with pytest.raises(ManagerStateError):
                        await manager.reserve(
                            {"memory": "1GB"}, wait=True, wait_timeout_seconds=None
                        )

                tasks.start_soon(waiter)
                await _until(manager, lambda state: _beats(state) >= 1)
                await manager.close()
        finally:
            await manager.close()
    if anyio_backend == "asyncio":
        assert len(asyncio.all_tasks()) == before_tasks
    with anyio.fail_after(5):
        while threads() - before_threads:  # noqa: ASYNC110 - polls thread exit
            await anyio.sleep(0.01)


_OUTCOMES = st.sampled_from(["admitted", "cancelled", "timed_out", "shutdown", "rejected"])


@settings_hypothesis(max_examples=200, deadline=None)
@given(
    interval=st.sampled_from([0.05, 1.0, None]),
    steps=st.lists(
        st.one_of(
            st.tuples(st.just("advance"), st.floats(0, 5, allow_nan=False)),
            st.tuples(st.just("beat"), st.integers(0, 3)),
            st.tuples(st.just("leave"), st.integers(0, 3), _OUTCOMES),
        ),
        max_size=60,
    ),
)
def test_heartbeat_and_leave_lifecycle_invariants(
    interval: float | None, steps: list[tuple[Any, ...]]
) -> None:
    now = [1000.0]
    coordinator: Any = SimpleNamespace(
        config=SimpleNamespace(admission_heartbeat_interval_seconds=interval),
        _heartbeat_count=0,
        _missed_heartbeat_count=0,
        _max_heartbeat_delay=None,
        _last_heartbeat=None,
        _admitted_waiter_count=0,
        _shutdown_waiter_count=0,
        _completed_wait_count=0,
        _total_wait_seconds=0.0,
        _notify=lambda: None,
    )
    coordinator._record_wait = partial(AdmissionCoordinator._record_wait, coordinator)
    deadline = now[0] + 3
    waiters = [
        _Waiter(
            0,
            index + 1,
            now[0],
            timeout=None if index % 2 else 3.0,
            deadline=None if index % 2 else deadline,
            next_heartbeat=float("inf") if interval is None else now[0] + interval,
        )
        for index in range(4)
    ]
    coordinator._waiters = list(waiters)
    first_outcome: dict[int, str] = {}
    with patch("resourcera.admission.time", SimpleNamespace(monotonic=lambda: now[0])):
        for step in steps:
            if step[0] == "advance":
                now[0] += step[1]
            elif step[0] == "beat":
                waiter = waiters[step[1]]
                before = waiter.heartbeats
                AdmissionCoordinator._heartbeat(coordinator, waiter)
                if waiter.outcome != "waiting" or interval is None:
                    assert waiter.heartbeats == before  # cannot revive or fabricate
                if waiter.heartbeats > before:
                    assert interval is not None
                    assert waiter.last_heartbeat == now[0]
                    assert waiter.next_heartbeat == now[0] + interval
            else:
                waiter = waiters[step[1]]
                AdmissionCoordinator._leave(coordinator, waiter, step[2])
                first_outcome.setdefault(step[1], step[2])
            for index, waiter in enumerate(waiters):
                # Heartbeats never touch deadlines, and an outcome is set exactly once.
                assert waiter.deadline == (None if index % 2 else deadline)
                assert waiter.outcome == first_outcome.get(index, "waiting")
                assert (waiter in coordinator._waiters) == (waiter.outcome == "waiting")
                assert waiter.missed_heartbeats >= 0
    assert coordinator._heartbeat_count == sum(waiter.heartbeats for waiter in waiters)
    assert coordinator._missed_heartbeat_count == sum(w.missed_heartbeats for w in waiters)
    assert coordinator._completed_wait_count == len(first_outcome)
    outcomes = list(first_outcome.values())
    assert coordinator._admitted_waiter_count == outcomes.count("admitted")
    assert coordinator._shutdown_waiter_count == outcomes.count("shutdown")
