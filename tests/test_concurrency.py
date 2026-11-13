import threading
import time
from concurrent.futures import ThreadPoolExecutor

import anyio
import anyio.lowlevel
import pytest

from resourcera import (
    AdmissionRejectedError,
    ConcurrencyState,
    ManagerStateError,
    ReservationError,
    ResourceConfig,
    ResourceManager,
    WorkloadProfile,
    WorkloadRequirements,
)
from resourcera.metrics import SampleClock, metric
from resourcera.models import ResourceSnapshot, Scope

GB = 1_000_000_000
MEMORY: dict[str, object] = {"memory": GB, "safety_margin": 0}


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


def settings(**values: object) -> ResourceConfig:
    return ResourceConfig.model_validate(
        {
            "cpu_memory_interval_seconds": 60,
            "disk_network_interval_seconds": 60,
            "cpu_budget_fraction": 1,
            "concurrency_adjustment_interval_seconds": 60,
            "admission_wait_timeout_seconds": 0.5,
            **values,
        }
    )


async def start_manager(snapshot: ResourceSnapshot, config: ResourceConfig) -> ResourceManager:
    manager = ResourceManager(config)
    await manager.start()
    assert manager._runtime is not None
    # Keep background samples from replacing the injected snapshot on slow runners.
    manager._runtime._publish = lambda sample: None
    manager._runtime._snapshot = snapshot
    return manager


def use(manager: ResourceManager, snapshot: ResourceSnapshot) -> None:
    assert manager._runtime is not None
    manager._runtime._snapshot = snapshot


def stale_memory(snapshot: ResourceSnapshot) -> ResourceSnapshot:
    metric = snapshot.memory.cgroup_available_bytes
    old = metric.model_copy(
        update={"sampled_monotonic": time.monotonic() - metric.fresh_for_seconds - 3600}
    )
    return snapshot.model_copy(
        update={"memory": snapshot.memory.model_copy(update={"cgroup_available_bytes": old})}
    )


def measured[T](value: T, unit: str):
    return metric(value, unit, "test", Scope.MACHINE, SampleClock.now(), 60)


def with_memory(snapshot: ResourceSnapshot, available: int) -> ResourceSnapshot:
    memory = snapshot.memory.model_copy(
        update={"cgroup_available_bytes": measured(available, "bytes")}
    )
    return snapshot.model_copy(update={"memory": memory})


def with_pressure(snapshot: ResourceSnapshot, **percent: float) -> ResourceSnapshot:
    pressure = snapshot.pressure.model_copy(
        update={f"{name}_percent": measured(value, "percent") for name, value in percent.items()}
    )
    return snapshot.model_copy(update={"pressure": pressure})


async def eventually(condition) -> None:
    with anyio.fail_after(5):
        while not condition():  # noqa: ASYNC110 - polls controller state observed by tests
            await anyio.sleep(0.005)


@pytest.mark.anyio
async def test_recommendation_uses_capacity_bounds_and_reservations(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(snapshot, settings())
    try:
        recommendation = await manager.recommend_concurrency(MEMORY, max_concurrency=32)
        assert recommendation.recommended_limit == recommendation.estimated_limit == 3
        assert recommendation.limiting_resource == "memory"
        assert recommendation.capacity_known
        assert recommendation.confidence == pytest.approx(0.7)
        assert recommendation.snapshot_id == snapshot.id
        assert recommendation.safety_margin == 0

        capped = await manager.recommend_concurrency(MEMORY, max_concurrency=2)
        assert capped.recommended_limit == 2
        assert capped.limiting_resource == "requested_maximum"

        lease = await manager.reserve(MEMORY)
        reduced = await manager.recommend_concurrency(MEMORY, max_concurrency=32)
        assert reduced.recommended_limit == 2
        await manager.release(lease)

        none_left = await manager.recommend_concurrency(
            {"memory": 4 * GB, "safety_margin": 0}, min_concurrency=2, max_concurrency=8
        )
        assert none_left.estimated_limit == 0
        assert none_left.recommended_limit == 2
        assert "minimum" in none_left.reason

        with pytest.raises(ValueError, match="bounds"):
            await manager.recommend_concurrency(MEMORY, min_concurrency=3, max_concurrency=2)
        with pytest.raises(ValueError, match="bounds"):
            await manager.recommend_concurrency(MEMORY, min_concurrency=0)
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_worker_recommendations_follow_workload_requirements(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(
        snapshot,
        settings(
            network_capacity_bytes_per_second=1_010,
            disk_read_capacity_bytes_per_second=1_010,
            disk_write_capacity_bytes_per_second=1_020,
        ),
    )
    try:
        cpu = await manager.recommend_concurrency(
            {"cpu": 0.5, "safety_margin": 0, "profile": WorkloadProfile.CPU_BOUND},
            max_concurrency=32,
        )
        assert cpu.limiting_resource == "cpu"
        assert cpu.recommended_limit == 3
        io = await manager.recommend_concurrency(
            {"memory": 100_000_000, "safety_margin": 0, "profile": "io-bound"},
            max_concurrency=32,
        )
        assert io.recommended_limit == 30
        assert io.limiting_resource == "memory"
        network = await manager.recommend_concurrency(
            {"network_bytes_per_second": 100, "safety_margin": 0, "profile": "network-bound"},
            max_concurrency=200,
        )
        assert network.recommended_limit == 10
        assert any("does not determine request concurrency" in a for a in network.assumptions)
        disk = await manager.recommend_concurrency(
            {
                "disk_read_bytes_per_second": 100,
                "disk_write_bytes_per_second": 200,
                "safety_margin": 0,
                "profile": WorkloadProfile.DISK_BOUND,
            },
            max_concurrency=40,
        )
        assert (disk.recommended_limit, disk.limiting_resource) == (5, "disk_write")
        assert any("queue depth" in a for a in disk.assumptions)
        assert any("latency" in a.lower() for a in disk.assumptions)
        mixed = await manager.recommend_concurrency(
            {"cpu": 0.25, "memory": 600_000_000, "safety_margin": 0, "profile": "mixed"},
            max_concurrency=32,
        )
        assert (mixed.recommended_limit, mixed.limiting_resource) == (5, "memory")

        use(manager, with_pressure(snapshot, io=50))
        pressured = await manager.recommend_concurrency(
            {"disk_read_bytes_per_second": 100, "safety_margin": 0}, max_concurrency=40
        )
        assert pressured.recommended_limit == 5
        use(manager, with_pressure(snapshot, memory=50))
        assert (await manager.recommend_concurrency(MEMORY)).recommended_limit == 1
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_unknown_stale_and_accelerator_capacity_is_never_unlimited(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(snapshot, settings())
    try:
        cases: tuple[tuple[dict[str, object], str], ...] = (
            ({"network_bytes_per_second": 1_000}, "network"),
            ({"disk_write_bytes_per_second": 1_000}, "disk_write"),
        )
        for requirements, resource in cases:
            unknown = await manager.recommend_concurrency(
                requirements, min_concurrency=2, max_concurrency=50
            )
            assert not unknown.capacity_known
            assert unknown.estimated_limit is None
            assert unknown.recommended_limit == 2
            assert unknown.confidence == 0
            assert unknown.limiting_resource == resource
            assert "ceiling" in unknown.reason

        use(manager, stale_memory(snapshot))
        stale_result = await manager.recommend_concurrency(MEMORY, max_concurrency=10)
        assert not stale_result.capacity_known
        assert stale_result.recommended_limit == 1

        use(manager, snapshot)
        accelerator = await manager.recommend_concurrency({"accelerator_count": 1})
        assert accelerator.recommended_limit == 1
        assert accelerator.limiting_resource == "accelerators"

        async with manager.adaptive_concurrency({"network_bytes_per_second": 1_000}) as limiter:
            with pytest.raises(AdmissionRejectedError):
                async with limiter.slot():
                    pass
            assert limiter.state.active_count == 0
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_container_limits_bound_recommendations(snapshot: ResourceSnapshot) -> None:
    manager = await start_manager(snapshot, settings())
    try:
        result = await manager.recommend_concurrency(
            {"cpu": 1, "safety_margin": 0}, max_concurrency=16
        )
        assert result.recommended_limit == 1
        assert result.limiting_resource == "cpu"
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_slots_hold_leases_and_reduced_limits_do_not_cancel_work(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(snapshot, settings())
    try:
        async with manager.adaptive_concurrency(MEMORY, max_concurrency=8) as controller:
            assert controller.state.current_limit == 3
            assert "Initial limit" in controller.state.adjustment_reason
            release = anyio.Event()
            finished: list[int] = []

            async def work(index: int) -> None:
                async with controller.slot() as decision:
                    assert decision.accepted
                    await release.wait()
                    finished.append(index)

            async with anyio.create_task_group() as group:
                for index in range(2):
                    group.start_soon(work, index)
                await eventually(lambda: controller.state.active_count == 2)
                assert (await manager.capacity(MEMORY)).max_concurrency == 1

                use(manager, with_memory(snapshot, GB))
                state = await controller.adjust()
                assert state.current_limit == 2
                assert state.active_count == 2
                assert state.available_permits == 0
                assert (state.history[-1].previous_limit, state.history[-1].new_limit) == (3, 2)

                controller.signal_overload("timeouts increased")
                assert controller.current_limit == 1
                assert controller.state.active_count == 2

                group.start_soon(work, 2)
                await eventually(lambda: controller.state.pending_count == 1)
                assert finished == []
                release.set()
            assert sorted(finished) == [0, 1, 2]
            assert controller.state.active_count == 0
            assert (await manager.capacity(MEMORY)).max_concurrency == 1
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_increases_need_hysteresis_and_the_adjustment_interval(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(
        with_memory(snapshot, 2 * GB), settings(concurrency_increase_step=2)
    )
    try:
        async with manager.adaptive_concurrency(
            MEMORY, min_concurrency=1, max_concurrency=5
        ) as controller:
            now = [time.monotonic()]
            controller._clock = lambda: now[0]
            assert controller.current_limit == 2
            use(manager, with_memory(snapshot, 10 * GB))
            assert (await controller.adjust()).current_limit == 2
            assert "confirmed" in controller.state.adjustment_reason
            assert (await controller.adjust()).current_limit == 2
            now[0] += 60
            assert (await controller.adjust()).current_limit == 4
            now[0] += 60
            await controller.adjust()
            state = await controller.adjust()
            assert state.current_limit == 5, state.adjustment_reason
            assert state.limiting_resource == "requested_maximum"
            assert [item.new_limit for item in state.history] == [2, 4, 5]

            controller.signal_overload("remote service returned HTTP 429")
            assert controller.current_limit == 2
            assert "HTTP 429" in controller.state.adjustment_reason
            controller.signal_overload()
            controller.signal_overload()
            assert controller.current_limit == 1

            use(manager, with_memory(snapshot, 0))
            assert (await controller.adjust()).current_limit == 1
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_unknown_or_degraded_measurements_hold_the_limit(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(with_memory(snapshot, 2 * GB), settings())
    try:
        async with manager.adaptive_concurrency(MEMORY, max_concurrency=10) as controller:
            controller._clock = lambda: time.monotonic() + 3600
            use(manager, stale_memory(snapshot))
            for _ in range(3):
                state = await controller.adjust()
            assert state.current_limit == 2
            assert state.confidence == 0
            assert "Holding" in state.adjustment_reason

            degraded = snapshot.overhead.model_copy(update={"degraded": True})
            use(manager, with_memory(snapshot, 9 * GB).model_copy(update={"overhead": degraded}))
            for _ in range(3):
                state = await controller.adjust()
            assert state.current_limit == 2
            assert state.confidence == pytest.approx(0.35)
            assert "degraded" in state.adjustment_reason

            use(manager, with_memory(snapshot, 9 * GB))
            await controller.adjust()
            state = await controller.adjust()
            assert state.current_limit == 3
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_first_measurable_capacity_calibrates_the_limit_directly(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(stale_memory(snapshot), settings())
    try:
        async with manager.adaptive_concurrency(MEMORY, max_concurrency=10) as controller:
            assert controller.current_limit == 1
            with pytest.raises(AdmissionRejectedError):
                async with controller.slot():
                    pass
            use(manager, with_memory(snapshot, 5 * GB))
            state = await controller.adjust()
            assert state.current_limit == 5
            assert state.adjustment_reason.startswith("Calibrated")
            use(manager, stale_memory(snapshot))
            state = await controller.adjust()
            assert state.current_limit == 5
            assert "Holding" in state.adjustment_reason
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_waiters_are_bounded_fair_and_cancellation_safe(snapshot: ResourceSnapshot) -> None:
    manager = await start_manager(snapshot, settings())
    try:
        async with manager.adaptive_concurrency(
            MEMORY, max_concurrency=1, max_waiters=2
        ) as controller:
            order: list[int] = []

            async def queued(index: int) -> None:
                async with controller.slot():
                    order.append(index)

            async with anyio.create_task_group() as group, controller.slot():
                group.start_soon(queued, 1)
                await eventually(lambda: controller.state.pending_count == 1)
                group.start_soon(queued, 2)
                await eventually(lambda: controller.state.pending_count == 2)
                with pytest.raises(AdmissionRejectedError, match="waiter queue is full"):
                    async with controller.slot():
                        pass
            assert order == [1, 2]

            async with controller.slot():
                with anyio.CancelScope() as scope:
                    scope.cancel()
                    async with controller.slot():
                        pytest.fail("a cancelled waiter must not be admitted")
                assert controller.state.pending_count == 0
            state = controller.state
            assert (state.active_count, state.pending_count, state.available_permits) == (0, 0, 1)
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_wait_timeout_and_slot_reuse(snapshot: ResourceSnapshot) -> None:
    manager = await start_manager(snapshot, settings())
    try:
        async with manager.adaptive_concurrency(
            MEMORY, max_concurrency=1, wait_timeout_seconds=0.05
        ) as controller:
            slot = controller.slot()
            async with slot:
                with pytest.raises(AdmissionRejectedError, match="timed out"):
                    async with controller.slot():
                        pass
            with pytest.raises(ManagerStateError, match="reused"):
                async with slot:
                    pass
            assert controller.state.available_permits == 1
        with pytest.raises(ValueError, match="max_waiters"):
            manager.adaptive_concurrency(MEMORY, max_waiters=0)
        with pytest.raises(ValueError, match="wait_timeout"):
            manager.adaptive_concurrency(MEMORY, wait_timeout_seconds=0)
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_controller_shutdown_rejects_waiters_and_preserves_active_work(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(snapshot, settings())
    try:
        stop = anyio.Event()
        errors: list[BaseException] = []

        async def owner(*, task_status=anyio.TASK_STATUS_IGNORED) -> None:
            async with manager.adaptive_concurrency(MEMORY, max_concurrency=1) as controller:
                task_status.started(controller)
                await stop.wait()

        async def waiter(controller) -> None:
            try:
                async with controller.slot():
                    pass
            except ManagerStateError as exc:
                errors.append(exc)

        async with anyio.create_task_group() as group:
            controller = await group.start(owner)
            held = controller.slot()
            await held.__aenter__()
            group.start_soon(waiter, controller)
            await eventually(lambda: controller.state.pending_count == 1)
            stop.set()
            await eventually(lambda: len(errors) == 1)
            assert controller.state.closed
            assert controller.state.active_count == 1
            await held.__aexit__(None, None, None)
        assert controller.state.active_count == 0
        with pytest.raises(ManagerStateError):
            async with controller.slot():
                pass
        with pytest.raises(ManagerStateError):
            await controller.adjust()
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_body_errors_propagate_without_exception_groups(snapshot: ResourceSnapshot) -> None:
    manager = await start_manager(snapshot, settings())
    try:
        with pytest.raises(LookupError, match="body"):
            async with manager.adaptive_concurrency(MEMORY):
                raise LookupError("body")
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_lost_slot_lease_raises_and_releases_the_permit(
    snapshot: ResourceSnapshot, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = await start_manager(snapshot, settings(lease_duration_seconds=0.3))
    try:
        async with manager.adaptive_concurrency(MEMORY, max_concurrency=2) as controller:
            assert manager._runtime is not None
            store = manager._runtime.coordinator()._store

            def lose(owner_id: str, lease_ids: tuple[str, ...]) -> dict[str, object]:
                store.release(owner_id, lease_ids)
                return {}

            monkeypatch.setattr(store, "renew", lose)

            async def work() -> int:
                async with controller.slot():
                    await anyio.sleep_forever()
                return 1

            with pytest.raises(ReservationError, match="lease was lost"), anyio.fail_after(2):
                await work()
            state = controller.state
            assert (state.active_count, state.available_permits) == (0, state.current_limit)
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_manager_shutdown_cancels_active_slots_and_closes_controllers(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(snapshot, settings(concurrency_adjustment_interval_seconds=0.01))
    async with manager.adaptive_concurrency(MEMORY, max_concurrency=2) as controller:
        outcome: list[str] = []

        async def active() -> None:
            try:
                async with controller.slot():
                    await anyio.sleep_forever()
            except ReservationError:
                outcome.append("lost")

        async with anyio.create_task_group() as group:
            group.start_soon(active)
            await eventually(lambda: controller.state.active_count == 1)
            await manager.close()
        assert outcome == ["lost"]
        await eventually(lambda: controller.state.closed)
        state = controller.state
        assert (state.active_count, state.available_permits) == (0, state.current_limit)


@pytest.mark.anyio
async def test_executor_submissions_are_bounded_and_released_after_completion(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(snapshot, settings())
    running = 0
    peak = 0
    lock = threading.Lock()

    def blocking(value: int) -> int:
        nonlocal running, peak
        with lock:
            running += 1
            peak = max(peak, running)
        time.sleep(0.02)
        with lock:
            running -= 1
        return value * 2

    def failing() -> None:
        raise OSError("disk unavailable")

    executor = ThreadPoolExecutor(max_workers=8)
    try:
        async with manager.adaptive_concurrency(
            MEMORY, max_concurrency=2, max_waiters=16
        ) as controller:
            results: list[int] = []

            async def submit(value: int) -> None:
                results.append(await controller.run_sync(executor, blocking, value))

            async with anyio.create_task_group() as group:
                for value in range(10):
                    group.start_soon(submit, value)
            assert sorted(results) == [value * 2 for value in range(10)]
            assert peak == 2
            with pytest.raises(OSError, match="disk unavailable"):
                await controller.run_sync(executor, failing)
            assert controller.state.active_count == 0
        assert executor.submit(blocking, 1).result() == 2
    finally:
        executor.shutdown(wait=True)
        await manager.close()


@pytest.mark.anyio
async def test_caller_cancellation_keeps_the_permit_until_the_thread_finishes(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(snapshot, settings())
    started = threading.Event()
    proceed = threading.Event()

    def blocking() -> str:
        started.set()
        proceed.wait(5)
        return "done"

    executor = ThreadPoolExecutor(max_workers=1)
    try:
        async with manager.adaptive_concurrency(MEMORY, max_concurrency=1) as controller:
            ticks = 0
            cancelled = anyio.Event()

            async def caller(scope: anyio.CancelScope) -> None:
                with scope:
                    await controller.run_sync(executor, blocking)
                cancelled.set()

            async with anyio.create_task_group() as group:
                scope = anyio.CancelScope()
                group.start_soon(caller, scope)
                await anyio.to_thread.run_sync(started.wait, 5)
                scope.cancel()
                for _ in range(5):
                    ticks += 1
                    await anyio.sleep(0.01)
                assert controller.state.active_count == 1
                assert controller.state.available_permits == 0
                assert not cancelled.is_set()
                proceed.set()
            assert cancelled.is_set()
            assert ticks == 5
            assert controller.state.active_count == 0

        executor.shutdown(wait=True)
        async with manager.adaptive_concurrency(MEMORY, max_concurrency=1) as controller:
            with pytest.raises(RuntimeError):
                await controller.run_sync(executor, blocking)
            assert controller.state.available_permits == 1
    finally:
        proceed.set()
        executor.shutdown(wait=True)
        await manager.close()


@pytest.mark.anyio
async def test_independent_controllers_share_reservation_accounting(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(snapshot, settings())
    try:
        async with (
            manager.adaptive_concurrency(MEMORY, max_concurrency=8) as first,
            manager.adaptive_concurrency(
                {"memory": GB, "cpu": 0.1, "safety_margin": 0}, max_concurrency=8
            ) as second,
        ):
            assert first.current_limit == second.current_limit == 3
            async with first.slot(), first.slot():
                state = await second.adjust()
                assert state.current_limit == 1
                async with second.slot():
                    with pytest.raises(AdmissionRejectedError):
                        async with first.slot():
                            pass
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_many_tasks_across_controllers_do_not_leak_permits_or_leases(
    snapshot: ResourceSnapshot,
) -> None:
    manager = await start_manager(snapshot, settings())
    small: dict[str, object] = {"memory": 10_000_000, "safety_margin": 0}
    try:
        async with (
            manager.adaptive_concurrency(small, max_concurrency=16, max_waiters=200) as first,
            manager.adaptive_concurrency(small, max_concurrency=8, max_waiters=200) as second,
        ):
            peaks = {id(first): 0, id(second): 0}

            async def work(controller) -> None:
                async with controller.slot():
                    peaks[id(controller)] = max(
                        peaks[id(controller)], controller.state.active_count
                    )
                    await anyio.lowlevel.checkpoint()

            async with anyio.create_task_group() as group:
                for index in range(300):
                    group.start_soon(work, first if index % 2 else second)
            assert peaks[id(first)] <= 16
            assert peaks[id(second)] <= 8
            for controller in (first, second):
                state = controller.state
                assert (state.active_count, state.pending_count) == (0, 0)
                assert state.available_permits == state.current_limit
                assert len(state.history) <= manager.config.concurrency_history_size
        assert (await manager.capacity(small)).max_concurrency == manager.config.max_concurrency
    finally:
        await manager.close()


@pytest.mark.anyio
async def test_adjustment_overhead_and_history_are_bounded(snapshot: ResourceSnapshot) -> None:
    manager = await start_manager(snapshot, settings(concurrency_history_size=4))
    try:
        async with manager.adaptive_concurrency(MEMORY, max_concurrency=3) as controller:
            now = [time.monotonic()]
            controller._clock = lambda: now[0]
            started = time.perf_counter()
            for _ in range(20):
                controller.signal_overload()
                now[0] += 60
                await controller.adjust()
                await controller.adjust()
            elapsed = time.perf_counter() - started
            assert elapsed / 40 < 0.05
            state = controller.state
            assert len(state.history) == 4
            assert isinstance(state, ConcurrencyState)
            assert set(state.pressure_percent) == {"cpu", "memory", "io"}
    finally:
        await manager.close()


def test_asyncio_application_usage_keeps_the_event_loop_responsive(
    snapshot: ResourceSnapshot,
) -> None:
    import asyncio

    async def main() -> tuple[int, int]:
        manager = await start_manager(snapshot, settings())
        ticks = 0
        stop = asyncio.Event()

        async def ticker() -> None:
            nonlocal ticks
            while not stop.is_set():
                ticks += 1
                await asyncio.sleep(0.005)

        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                async with manager.adaptive_concurrency(MEMORY, max_concurrency=2) as controller:
                    tick_task = asyncio.create_task(ticker())
                    results = await asyncio.gather(
                        *(controller.run_sync(executor, time.sleep, 0.05) for _ in range(4))
                    )
                    stop.set()
                    await tick_task
            return len(results), ticks
        finally:
            await manager.close()

    count, ticks = asyncio.run(main())
    assert count == 4
    assert ticks >= 5


def test_workload_requirements_accept_the_disk_profile() -> None:
    requirements = WorkloadRequirements(disk_read_bytes_per_second=1, profile="disk-bound")
    assert requirements.profile == WorkloadProfile.DISK_BOUND
    with pytest.raises(ValueError, match="at least one resource requirement"):
        WorkloadRequirements(profile="io-bound")
