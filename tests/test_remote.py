import math
import threading
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import anyio
import anyio.from_thread
import anyio.lowlevel
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from resourcera import (
    AdmissionRejectedError,
    ManagerStateError,
    RemoteConcurrencyController,
    RemoteConcurrencyRejectedError,
    ResourceConfig,
    ResourceManager,
)
from resourcera import remote as remote_module
from resourcera.models import ResourceSnapshot
from resourcera.remote import _retry_after_seconds


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


def fake_clock(controller: RemoteConcurrencyController) -> list[float]:
    now = [0.0]
    controller._clock = lambda: now[0]
    controller._window_started = 0.0
    return now


@pytest.mark.anyio
async def test_remote_feedback_is_burst_aggregated_and_recovers() -> None:
    async with (
        RemoteConcurrencyController(
            "first", initial_concurrency=3, max_concurrency=8, observation_window_seconds=1
        ) as first,
        RemoteConcurrencyController("second", initial_concurrency=3) as second,
    ):
        now = [0.0]
        first._clock = lambda: now[0]
        first._window_started = 0.0
        for _ in range(4):
            first.report_success(latency_seconds=0.1)
        assert first.state.current_limit == 3
        now[0] = 1.0
        first.report_success(latency_seconds=0.1)
        assert first.state.current_limit == 4
        first.report_overload(status_code=429)
        first.report_overload(status_code=503)
        assert first.state.current_limit == 2
        assert first.state.overload_count == 2
        assert second.state.current_limit == 3
        assert second.state.history == ()
        first.report_capacity(2)
        assert first.state.authoritative_capacity == 2
        first.report_failure(kind="authentication")
        assert first.state.current_limit == 2
        now[0] = 3.0
        for _ in range(4):
            first.report_success(latency_seconds=0.1)
        assert first.state.current_limit == 2
        now[0] = 4.0
        for _ in range(4):
            first.report_success(latency_seconds=0.1)
        assert first.state.current_limit == 2
        first.report_capacity(None)
        now[0] = 5.0
        for _ in range(4):
            first.report_success(latency_seconds=0.1)
        assert first.state.current_limit == 3
        assert [change.new_limit for change in first.state.history] == [4, 2, 3]


@pytest.mark.anyio
async def test_latency_and_non_overload_failures_prevent_increase() -> None:
    async with RemoteConcurrencyController("endpoint", minimum_samples=2) as controller:
        now = [0.0]
        controller._clock = lambda: now[0]
        controller._window_started = 0.0
        controller.report_success(latency_seconds=0.1)
        controller.report_failure(kind="invalid_request")
        now[0] = 1.0
        controller.report_success(latency_seconds=2.0)
        assert controller.state.current_limit == 1
        assert controller.state.overload_count == 0
        assert "Holding" in controller.state.adjustment_reason
        with pytest.raises(ValueError):
            controller.report_overload(status_code=401)
        with pytest.raises(ValueError):
            controller.report_success(latency_seconds=math.nan)


@pytest.mark.anyio
async def test_remote_queue_wait_inhibits_ramp_until_it_recovers() -> None:
    async with RemoteConcurrencyController(
        "endpoint", initial_concurrency=1, max_concurrency=4, minimum_samples=2
    ) as controller:
        now = [0.0]
        controller._clock = lambda: now[0]
        controller._window_started = 0.0
        controller.report_success(latency_seconds=0.1, remote_queue_wait_seconds=0.01)
        now[0] = 1.0
        controller.report_success(latency_seconds=0.1, remote_queue_wait_seconds=0.5)
        assert controller.state.current_limit == 1
        assert controller.state.remote_queue_wait_seconds == pytest.approx(0.108)
        assert "remote queue wait" in controller.state.adjustment_reason
        for _ in range(20):
            controller.report_success(latency_seconds=0.1, remote_queue_wait_seconds=0.01)
        now[0] = 2.0
        controller.report_success(latency_seconds=0.1, remote_queue_wait_seconds=0.01)
        assert controller.state.current_limit == 2
        before = controller.state.success_count
        with pytest.raises(ValueError, match="remote_queue_wait_seconds"):
            controller.report_success(latency_seconds=0.1, remote_queue_wait_seconds=math.nan)
        assert controller.state.success_count == before


def test_retry_after_validation() -> None:
    assert _retry_after_seconds("5") == 5
    date = format_datetime(datetime.now(UTC) + timedelta(seconds=20), usegmt=True)
    assert 18 <= _retry_after_seconds(date) <= 20
    for value in ("-1", "1.5", "invalid", "86401"):
        with pytest.raises(ValueError):
            _retry_after_seconds(value)


@pytest.mark.anyio
async def test_remote_queue_timeout_cancellation_and_shutdown() -> None:
    async with RemoteConcurrencyController(
        "endpoint", max_waiters=1, wait_timeout_seconds=0.1
    ) as controller:
        finished = anyio.Event()

        async def waiter() -> None:
            with pytest.raises(RemoteConcurrencyRejectedError, match="timed out"):
                async with controller.slot():
                    pass
            finished.set()

        async with controller.slot(), anyio.create_task_group() as tasks:
            tasks.start_soon(waiter)
            with anyio.fail_after(2):
                while controller.state.pending_count != 1:  # noqa: ASYNC110
                    await anyio.sleep(0.001)
            with pytest.raises(RemoteConcurrencyRejectedError, match="queue is full"):
                async with controller.slot():
                    pass
            with anyio.fail_after(2):
                await finished.wait()
        assert (controller.state.in_flight, controller.state.pending_count) == (0, 0)

    manager = ResourceManager()
    controller = manager.remote_concurrency("shutdown")
    async with controller:
        async with controller.slot():
            errors: list[Exception] = []

            async def waiting() -> None:
                try:
                    async with controller.slot():
                        pytest.fail("closed remote controller admitted work")
                except ManagerStateError as exc:
                    errors.append(exc)

            async with anyio.create_task_group() as tasks:
                tasks.start_soon(waiting)
                with anyio.fail_after(2):
                    while controller.state.pending_count != 1:  # noqa: ASYNC110
                        await anyio.sleep(0.001)
                await manager.close()
            assert len(errors) == 1
        assert controller.state.in_flight == 0


@pytest.mark.anyio
async def test_retry_after_delays_new_requests_without_blocking_event_loop() -> None:
    async with RemoteConcurrencyController(
        "endpoint", initial_concurrency=2, wait_timeout_seconds=0.1
    ) as controller:
        controller.report_overload(status_code=429, retry_after="1")
        ticked = anyio.Event()

        async def tick() -> None:
            await anyio.sleep(0.01)
            ticked.set()

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(tick)
            with pytest.raises(RemoteConcurrencyRejectedError, match="timed out"):
                async with controller.slot():
                    pytest.fail("Retry-After must prevent a new request")
        assert ticked.is_set()
        assert controller.state.in_flight == 0
        assert controller.state.pending_count == 0


@pytest.mark.anyio
async def test_cancellation_during_cooldown_releases_remote_permit() -> None:
    async with RemoteConcurrencyController("endpoint", cooldown_seconds=0.1) as controller:
        controller.report_overload(status_code=503)
        done = anyio.Event()
        scopes: list[anyio.CancelScope] = []

        async def waiter() -> None:
            with anyio.CancelScope() as scope:
                scopes.append(scope)
                try:
                    async with controller.slot():
                        pytest.fail("cancelled remote waiter was admitted")
                finally:
                    done.set()

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(waiter)
            with anyio.fail_after(2):
                while controller.state.pending_count != 1:  # noqa: ASYNC110
                    await anyio.sleep(0.001)
            scopes[0].cancel()
            with anyio.fail_after(2):
                await done.wait()
        await anyio.sleep(0.11)
        with anyio.fail_after(2):
            async with controller.slot():
                assert controller.state.in_flight == 1
        assert controller.state.in_flight == 0


@pytest.mark.anyio
async def test_many_remote_calls_remain_bounded() -> None:
    async with RemoteConcurrencyController(
        "gateway",
        initial_concurrency=3,
        max_concurrency=8,
        max_waiters=200,
        observation_window_seconds=60,
    ) as controller:
        active = 0
        peak = 0
        completed = 0

        async def call() -> None:
            nonlocal active, peak, completed
            async with controller.slot():
                active += 1
                peak = max(peak, active)
                await anyio.sleep(0.001)
                active -= 1
                completed += 1
                controller.report_success(latency_seconds=0.001)

        with anyio.fail_after(10):
            async with anyio.create_task_group() as tasks:
                for _ in range(200):
                    tasks.start_soon(call)
        assert completed == 200
        assert peak <= 3
        assert controller.state.in_flight == controller.state.pending_count == 0


@pytest.mark.anyio
@pytest.mark.parametrize("total", [40, 200, 500, 1000])
async def test_remote_bounded_producers_complete_work(total: int) -> None:
    async with RemoteConcurrencyController(
        "gateway", initial_concurrency=3, max_waiters=40, observation_window_seconds=60
    ) as controller:
        completed = 0
        peak = 0

        async def producer(index: int) -> None:
            nonlocal completed, peak
            for _ in range(index, total, 40):
                async with controller.slot():
                    peak = max(peak, controller.state.in_flight)
                    await anyio.sleep(0.001)
                    completed += 1
                    controller.report_success(latency_seconds=0.001)

        with anyio.fail_after(10):
            async with anyio.create_task_group() as tasks:
                for index in range(min(total, 40)):
                    tasks.start_soon(producer, index)
        assert completed == total
        assert peak <= 3
        assert controller.state.in_flight == controller.state.pending_count == 0


@pytest.mark.anyio
async def test_simulated_gateway_capacity_drop_and_recovery() -> None:
    async with RemoteConcurrencyController(
        "gateway",
        initial_concurrency=3,
        max_concurrency=16,
        observation_window_seconds=1,
        cooldown_seconds=2,
    ) as controller:
        now = [0.0]
        controller._clock = lambda: now[0]
        controller._window_started = 0.0
        overloads = 0
        naive_overloads = 0
        limits: list[int] = []
        for capacity in (6,) * 20 + (3,) * 15 + (6,) * 25:
            now[0] += 1
            limit = controller.state.current_limit
            limits.append(limit)
            naive_overloads += 16 - capacity
            for _ in range(min(limit, capacity)):
                controller.report_success(latency_seconds=0.1)
            for _ in range(max(0, limit - capacity)):
                overloads += 1
                controller.report_overload(status_code=429)
        assert max(limits[:20]) >= 6
        assert min(limits[20:35]) <= 3
        assert max(limits[35:]) >= 6
        assert overloads < naive_overloads
        assert controller.state.overload_count == overloads


@pytest.mark.anyio
async def test_feedback_is_bound_to_the_owning_event_loop_thread() -> None:
    async with RemoteConcurrencyController("endpoint", minimum_samples=2) as controller:

        def direct() -> None:
            controller.report_success(latency_seconds=0.1)

        with pytest.raises(RuntimeError, match="from_thread"):
            await anyio.to_thread.run_sync(direct)
        for call in (
            lambda: controller.report_overload(status_code=429),
            lambda: controller.report_failure(kind="authentication"),
            lambda: controller.report_capacity(1),
        ):
            with pytest.raises(RuntimeError):
                await anyio.to_thread.run_sync(call)
        state = controller.state
        assert (state.success_count, state.overload_count, state.failure_count) == (0, 0, 0)

        def marshalled() -> None:
            anyio.from_thread.run_sync(lambda: controller.report_success(latency_seconds=0.1))
            anyio.from_thread.run_sync(lambda: controller.report_overload(status_code=503))

        await anyio.to_thread.run_sync(marshalled)
        assert (controller.state.success_count, controller.state.overload_count) == (1, 1)


@pytest.mark.anyio
async def test_controller_created_off_loop_binds_on_first_loop_use() -> None:
    controller = await anyio.to_thread.run_sync(lambda: RemoteConcurrencyController("lazy"))
    async with controller:
        async with controller.slot():
            assert controller.state.in_flight == 1
        with pytest.raises(RuntimeError):
            await anyio.to_thread.run_sync(lambda: controller.report_capacity(1))
        controller.report_capacity(1)
        assert controller.state.authoritative_capacity == 1


@pytest.mark.anyio
async def test_remote_controller_operations_reject_foreign_threads() -> None:
    controller = RemoteConcurrencyController("endpoint", initial_concurrency=2)

    async def foreign(call: Callable[[], Awaitable[object]]) -> None:
        # A separate event loop in a worker thread: a real but unsupported caller.
        with pytest.raises(RuntimeError, match="from_thread"):
            await anyio.to_thread.run_sync(anyio.run, call)

    async with controller:
        slot = controller.slot()
        await slot.__aenter__()
        with pytest.raises(RuntimeError, match="from_thread"):
            await anyio.to_thread.run_sync(lambda: controller.state)
        await foreign(lambda: slot.__aexit__(None, None, None))
        await foreign(lambda: controller.__aexit__(None, None, None))
        await foreign(lambda: controller.slot().__aenter__())
        state = controller.state
        assert (state.in_flight, state.pending_count, state.closed) == (1, 0, False)
        await slot.__aexit__(None, None, None)
        assert controller.state.in_flight == 0
    assert controller.state.closed
    other = RemoteConcurrencyController("other")
    await foreign(other.__aenter__)
    assert not other._entered


@pytest.mark.anyio
async def test_manager_close_from_another_thread_wakes_waiters_on_the_owner_loop() -> None:
    # Created outside any event loop so the manager can be closed from another loop.
    manager = await anyio.to_thread.run_sync(ResourceManager)
    controller = manager.remote_concurrency("endpoint", max_waiters=2)
    outcomes: list[str] = []

    async def waiter() -> None:
        with pytest.raises(ManagerStateError, match="closed while waiting"):
            async with controller.slot():
                pytest.fail("a closed controller admitted work")
        outcomes.append("woken")

    async with controller, controller.slot():
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(waiter)
            with anyio.fail_after(5):
                while controller.state.pending_count != 1:
                    await anyio.lowlevel.checkpoint()
            await anyio.to_thread.run_sync(anyio.run, manager.close)
            # The flag is set at once; waiter cancellation runs on this loop.
            assert controller.state.closed
            with pytest.raises(ManagerStateError):
                controller.report_success(latency_seconds=0.1)
        assert controller.state.in_flight == 1
    assert outcomes == ["woken"]
    assert (controller.state.in_flight, controller.state.pending_count) == (0, 0)


def _race_first_use(controller: RemoteConcurrencyController, racers: int) -> list[bool]:
    barrier = threading.Barrier(racers)
    results: list[bool] = []
    token = remote_module._current_token

    def synchronized_token() -> object:
        # Every racer observes an unbound owner before any of them can bind it.
        barrier.wait(10)
        return token()

    async def use() -> None:
        controller.report_success(latency_seconds=0.1)

    def race() -> None:
        try:
            anyio.run(use)
        except RuntimeError:
            results.append(False)
        else:
            results.append(True)
            winners.append(threading.get_ident())

    winners: list[int] = []
    threads = [threading.Thread(target=race) for _ in range(racers)]
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(remote_module, "_current_token", synchronized_token)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
    assert winners == [controller._owner._thread]
    return results


def test_simultaneous_first_use_binds_exactly_one_owner() -> None:
    for _ in range(3):
        controller = RemoteConcurrencyController("race")
        results = _race_first_use(controller, 8)
        assert sorted(results) == [False] * 7 + [True]
        owner = controller._owner._thread
        # Rejected threads changed nothing; ownership stays with the winner.
        assert controller._successes == 1
        assert len(controller._samples) == 1
        with pytest.raises(RuntimeError, match="from_thread"):
            anyio.run(controller.__aenter__)
        with pytest.raises(RuntimeError, match="from_thread"):
            controller.state  # noqa: B018 - property access is the operation under test
        assert controller._owner._thread == owner


@pytest.mark.anyio
async def test_concurrent_feedback_from_many_tasks_is_consistent() -> None:
    async with RemoteConcurrencyController(
        "endpoint", initial_concurrency=4, max_concurrency=8, max_waiters=200
    ) as controller:

        async def call(index: int) -> None:
            async with controller.slot():
                await anyio.lowlevel.checkpoint()
                if index % 10 == 0:
                    controller.report_overload(status_code=429)
                elif index % 10 == 1:
                    controller.report_failure(kind="application_error")
                else:
                    controller.report_success(latency_seconds=0.01)

        async with anyio.create_task_group() as tasks:
            for index in range(200):
                tasks.start_soon(call, index)
        state = controller.state
        assert (state.success_count, state.overload_count, state.failure_count) == (160, 20, 20)
        assert 1 <= state.current_limit <= 8
        assert (state.in_flight, state.pending_count) == (0, 0)


@pytest.mark.anyio
async def test_feedback_racing_shutdown_is_rejected_without_corrupting_state() -> None:
    manager = ResourceManager()
    controller = manager.remote_concurrency("endpoint", initial_concurrency=2)
    async with controller:
        async with controller.slot():
            controller.report_success(latency_seconds=0.1)
            await manager.close()
            for call in (
                lambda: controller.report_success(latency_seconds=0.1),
                lambda: controller.report_overload(status_code=429),
                lambda: controller.report_failure(kind="authentication"),
                lambda: controller.report_capacity(1),
            ):
                with pytest.raises(ManagerStateError):
                    call()
            with pytest.raises(RuntimeError):
                await anyio.to_thread.run_sync(lambda: controller.report_capacity(1))
            assert controller.state.in_flight == 1
        state = controller.state
        assert (state.in_flight, state.success_count, state.overload_count) == (0, 1, 0)
        assert (state.current_limit, state.closed) == (2, True)
        with pytest.raises(ManagerStateError):
            async with controller.slot():
                pytest.fail("a closed controller admitted work")
    with pytest.raises(ManagerStateError):
        manager.remote_concurrency("late")


@pytest.mark.anyio
async def test_low_volume_feedback_needs_minimum_fresh_samples_not_a_full_window() -> None:
    async with RemoteConcurrencyController(
        "quiet",
        initial_concurrency=1,
        max_concurrency=4,
        minimum_samples=4,
        sample_max_age_seconds=60,
    ) as controller:
        now = fake_clock(controller)
        for _ in range(3):
            now[0] += 15
            controller.report_success(latency_seconds=0.1)
        assert controller.state.current_limit == 1
        assert controller.state.history == ()
        now[0] += 15
        controller.report_success(latency_seconds=0.1)
        assert controller.state.current_limit == 2

    async with RemoteConcurrencyController("idle", initial_concurrency=3) as idle:
        now = fake_clock(idle)
        now[0] = 86_400
        state = idle.state
        assert (state.current_limit, state.history) == (3, ())
        assert "awaiting remote feedback" in state.adjustment_reason


@pytest.mark.anyio
async def test_stale_samples_expire_and_lose_influence() -> None:
    async with RemoteConcurrencyController(
        "sparse", initial_concurrency=1, minimum_samples=4, sample_max_age_seconds=60
    ) as controller:
        now = fake_clock(controller)
        # Outcomes spaced beyond the maximum age never accumulate into evidence.
        for _ in range(10):
            now[0] += 600
            controller.report_success(latency_seconds=0.1)
        assert controller.state.current_limit == 1
        assert len(controller._samples) == 1

    async with RemoteConcurrencyController(
        "recovered", initial_concurrency=2, minimum_samples=4, sample_max_age_seconds=60
    ) as controller:
        now = fake_clock(controller)
        controller.report_success(latency_seconds=0.1)
        for _ in range(3):
            controller.report_failure(kind="application_error")
        # Old failures would hold the limit; once they expire, fresh successes decide.
        now[0] = 120
        for _ in range(4):
            controller.report_success(latency_seconds=0.1)
        assert controller.state.current_limit == 3

    async with RemoteConcurrencyController(
        "slow-then-fast", initial_concurrency=2, minimum_samples=4, sample_max_age_seconds=60
    ) as controller:
        now = fake_clock(controller)
        controller.report_success(latency_seconds=0.1)
        now[0] = 1
        for _ in range(3):
            controller.report_success(latency_seconds=5.0)
        # Recent slow outcomes count, so the window holds.
        assert controller.state.current_limit == 2
        assert "Holding" in controller.state.adjustment_reason
        now[0] = 2
        for _ in range(4):
            controller.report_success(latency_seconds=0.1)
        # The healthy window is judged on its own latency, not on a smoothed history.
        assert controller.state.current_limit == 3


@pytest.mark.anyio
async def test_observation_window_memory_is_bounded() -> None:
    async with RemoteConcurrencyController(
        "flood", initial_concurrency=2, max_samples=16, cooldown_seconds=1
    ) as controller:
        now = fake_clock(controller)
        controller.report_overload(status_code=429)
        for _ in range(10_000):
            controller.report_success(latency_seconds=0.01)
            controller.report_failure(kind="invalid_request")
        assert len(controller._samples) == 16
        assert controller.state.current_limit == 1
        now[0] = 1
        controller.report_success(latency_seconds=0.01)
        # Eight failures among the sixteen most recent outcomes hold the limit.
        assert controller.state.current_limit == 1
        assert len(controller._samples) == 0


def test_observation_window_parameters_are_validated() -> None:
    with pytest.raises(ValueError, match="max_samples"):
        RemoteConcurrencyController("x", minimum_samples=8, max_samples=4)
    with pytest.raises(ValueError, match="max_samples"):
        RemoteConcurrencyController("x", max_samples=5000)
    with pytest.raises(TypeError, match="max_samples"):
        RemoteConcurrencyController("x", max_samples=True)
    with pytest.raises(ValueError, match="sample_max_age_seconds"):
        RemoteConcurrencyController("x", observation_window_seconds=10, sample_max_age_seconds=5)
    with pytest.raises(ValueError, match="sample_max_age_seconds"):
        RemoteConcurrencyController("x", sample_max_age_seconds=math.inf)
    assert RemoteConcurrencyController("x", observation_window_seconds=20)._max_age == 80


@pytest.mark.anyio
async def test_high_volume_bursts_increase_at_most_once_per_window() -> None:
    async with RemoteConcurrencyController(
        "busy", initial_concurrency=2, max_concurrency=32, observation_window_seconds=1
    ) as controller:
        now = fake_clock(controller)
        for window in range(1, 4):
            now[0] = window - 0.5
            for _ in range(1000):
                controller.report_success(latency_seconds=0.01)
            assert controller.state.current_limit == 1 + window
            now[0] = window
            controller.report_success(latency_seconds=0.01)
            assert controller.state.current_limit == 2 + window
        assert len(controller.state.history) == 3


@pytest.mark.anyio
async def test_non_overload_failures_never_decrease_and_congestion_recovers() -> None:
    async with RemoteConcurrencyController(
        "endpoint", initial_concurrency=4, max_concurrency=8, cooldown_seconds=1
    ) as controller:
        now = fake_clock(controller)
        for kind in ("authentication", "invalid_request", "application_error"):
            controller.report_failure(kind=kind)
        assert (controller.state.current_limit, controller.state.overload_count) == (4, 0)
        controller.report_overload(reason="timeout")
        assert (controller.state.current_limit, controller.state.timeout_count) == (2, 1)
        now[0] = 0.5
        for _ in range(40):
            controller.report_success(latency_seconds=0.1)
        assert controller.state.current_limit == 2
        now[0] = 1.0
        controller.report_success(latency_seconds=0.1)
        assert controller.state.current_limit == 3
        now[0] = 2.0
        for _ in range(4):
            controller.report_success(latency_seconds=0.1)
        assert controller.state.current_limit == 4


@pytest.mark.anyio
async def test_retry_after_extends_cooldown_and_expiry_restores_fast_admission() -> None:
    async with RemoteConcurrencyController(
        "endpoint", initial_concurrency=2, cooldown_seconds=1
    ) as controller:
        now = fake_clock(controller)
        controller.report_overload(status_code=429, retry_after="5")
        assert controller.state.cooldown_remaining_seconds == 5
        controller.report_overload(status_code=503)
        assert controller.state.current_limit == 1
        assert controller.state.cooldown_remaining_seconds == 5
        now[0] = 5.0
        assert controller.state.cooldown_remaining_seconds == 0
        with anyio.fail_after(1):
            async with controller.slot():
                assert controller.state.in_flight == 1


@pytest.mark.anyio
async def test_capacity_drop_while_requests_are_active_drains_before_admitting() -> None:
    async with RemoteConcurrencyController(
        "endpoint", initial_concurrency=4, max_concurrency=8, max_waiters=4
    ) as controller:
        holders = [anyio.Event() for _ in range(4)]
        admitted = anyio.Event()

        async def hold(release: anyio.Event) -> None:
            async with controller.slot():
                await release.wait()

        async def late() -> None:
            async with controller.slot():
                admitted.set()

        async with anyio.create_task_group() as tasks:
            for release in holders:
                tasks.start_soon(hold, release)
            with anyio.fail_after(2):
                while controller.state.in_flight != 4:
                    await anyio.lowlevel.checkpoint()
            controller.report_capacity(2)
            assert (controller.state.current_limit, controller.state.in_flight) == (2, 4)
            tasks.start_soon(late)
            with anyio.fail_after(2):
                while controller.state.pending_count != 1:
                    await anyio.lowlevel.checkpoint()
            holders[0].set()
            holders[1].set()
            with anyio.fail_after(2):
                while controller.state.in_flight != 2:
                    await anyio.lowlevel.checkpoint()
            for _ in range(10):
                await anyio.lowlevel.checkpoint()
            assert not admitted.is_set()
            holders[2].set()
            with anyio.fail_after(2):
                await admitted.wait()
            holders[3].set()
        assert (controller.state.in_flight, controller.state.pending_count) == (0, 0)


@settings(max_examples=200, deadline=None)
@given(
    st.lists(
        st.one_of(
            st.tuples(st.just("success"), st.floats(0, 5)),
            st.tuples(st.just("overload"), st.sampled_from([429, 503])),
            st.tuples(st.just("failure"), st.sampled_from(["authentication", "application_error"])),
            st.tuples(st.just("capacity"), st.one_of(st.none(), st.integers(2, 12))),
            st.tuples(st.just("advance"), st.floats(0, 3)),
        ),
        max_size=80,
    )
)
def test_feedback_sequences_keep_the_limit_within_bounds(operations: list[tuple]) -> None:
    controller = RemoteConcurrencyController(
        "property", min_concurrency=2, initial_concurrency=3, max_concurrency=10
    )
    controller._owner._thread = threading.get_ident()
    now = fake_clock(controller)
    for name, value in operations:
        if name == "success":
            controller.report_success(latency_seconds=value)
        elif name == "overload":
            controller.report_overload(status_code=value)
        elif name == "failure":
            controller.report_failure(kind=value)
        elif name == "capacity":
            controller.report_capacity(value)
        else:
            now[0] += value
        state = controller.state
        assert 2 <= state.current_limit <= 10
        if state.authoritative_capacity is not None:
            assert state.current_limit <= state.authoritative_capacity
        for change in state.history:
            assert change.new_limit != change.previous_limit


async def _local_manager(snapshot: ResourceSnapshot) -> ResourceManager:
    manager = ResourceManager(
        ResourceConfig.model_validate(
            {"cpu_memory_interval_seconds": 60, "cpu_budget_fraction": 1, "max_concurrency": 2}
        )
    )
    await manager.start()
    assert manager._runtime is not None
    manager._runtime._snapshot = snapshot
    return manager


@pytest.mark.anyio
async def test_local_and_remote_admission_release_both_on_every_path(
    snapshot: ResourceSnapshot,
) -> None:
    memory = {"memory": "1GB"}
    manager = await _local_manager(snapshot)
    remote = manager.remote_concurrency("endpoint", wait_timeout_seconds=0.05)

    async def assert_no_leaks() -> None:
        state = await manager.admission_state()
        assert (state.active_reservations, state.queue_depth) == (0, 0)
        assert (remote.state.in_flight, remote.state.pending_count) == (0, 0)

    try:
        async with remote:
            async with remote.slot():
                with pytest.raises(RemoteConcurrencyRejectedError, match="timed out"):
                    async with manager.admission(memory), remote.slot():
                        pytest.fail("the remote permit is exhausted")
                assert (await manager.admission_state()).active_reservations == 0
                with anyio.CancelScope() as scope:
                    async with manager.admission(memory):
                        scope.cancel()
                        async with remote.slot():
                            pytest.fail("cancelled while waiting for the remote permit")
                assert scope.cancelled_caught
                assert (await manager.admission_state()).active_reservations == 0
            await assert_no_leaks()

            with pytest.raises(OSError, match="workload failed"):
                async with manager.admission(memory), remote.slot():
                    raise OSError("workload failed")
            await assert_no_leaks()

            with anyio.CancelScope() as scope:
                async with manager.admission(memory), remote.slot():
                    scope.cancel()
                    await anyio.sleep_forever()
            assert scope.cancelled_caught
            await assert_no_leaks()

            held = [await manager.reserve(memory) for _ in range(2)]
            with pytest.raises(AdmissionRejectedError):
                async with remote.slot(), manager.admission(memory):
                    pytest.fail("local capacity is exhausted")
            for lease in held:
                await manager.release(lease)
            await assert_no_leaks()

            for _ in range(50):
                async with manager.admission(memory), remote.slot():
                    remote.report_success(latency_seconds=0.01)
            await assert_no_leaks()
            assert remote.state.success_count == 50
    finally:
        await manager.close()
