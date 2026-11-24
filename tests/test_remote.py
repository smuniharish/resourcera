import math
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import anyio
import pytest

from resourcera import (
    ManagerStateError,
    RemoteConcurrencyController,
    RemoteConcurrencyRejectedError,
    ResourceManager,
)
from resourcera.remote import _retry_after_seconds


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


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
