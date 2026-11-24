import sqlite3
import threading
import time

import anyio
import anyio.lowlevel
import pytest

from resourcera import (
    AdmissionRejectedError,
    ManagerStateError,
    ReservationError,
    ResourceConfig,
    ResourceLease,
    ResourceManager,
    WorkloadRequirements,
)
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
            await manager.release(held)
        assert order == [10, 1]
        state = await manager.admission_state()
        assert state.queue_depth == 0
        assert state.completed_wait_count == 2
        assert state.total_wait_seconds > 0
        assert state.rejected_count == 2
    finally:
        await manager.close()


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
