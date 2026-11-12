import math
import time
from pathlib import Path

import anyio
import psutil
import pytest

from resourcera import ResourceConfig, ResourceManager
from resourcera.models import ResourceSnapshot


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


def percentile(samples: list[float], quantile: float) -> float:
    return sorted(samples)[math.ceil(len(samples) * quantile) - 1]


@pytest.mark.anyio
@pytest.mark.benchmark
async def test_cached_snapshot_and_admission_latency(
    snapshot: ResourceSnapshot, anyio_backend: str, tmp_path: Path
) -> None:
    for database in (None, tmp_path / f"{anyio_backend}.sqlite"):
        config = ResourceConfig(
            coordination_database=database,
            cpu_budget_fraction=1,
            cpu_memory_interval_seconds=60,
            max_concurrency=8,
        )
        async with ResourceManager(config) as manager:
            assert manager._runtime is not None
            manager._runtime._snapshot = snapshot
            snapshot_times: list[float] = []
            admission_times: list[float] = []
            for _ in range(100):
                start = time.perf_counter()
                await manager.snapshot()
                snapshot_times.append(time.perf_counter() - start)
                start = time.perf_counter()
                async with manager.admission({"memory": "1MB"}):
                    pass
                admission_times.append(time.perf_counter() - start)
            print(
                f"{anyio_backend} {'sqlite' if database else 'process'} "
                f"snapshot p50/p95/p99 ms: "
                f"{percentile(snapshot_times, 0.50) * 1000:.3f}/"
                f"{percentile(snapshot_times, 0.95) * 1000:.3f}/"
                f"{percentile(snapshot_times, 0.99) * 1000:.3f}; "
                f"admission p50/p95/p99 ms: "
                f"{percentile(admission_times, 0.50) * 1000:.3f}/"
                f"{percentile(admission_times, 0.95) * 1000:.3f}/"
                f"{percentile(admission_times, 0.99) * 1000:.3f}"
            )
            assert percentile(snapshot_times, 0.99) < 0.005
            assert percentile(admission_times, 0.95) < 0.15
            assert percentile(admission_times, 0.99) < 0.3


@pytest.mark.anyio
@pytest.mark.benchmark
async def test_sampling_and_cached_snapshot_footprint(anyio_backend: str) -> None:
    process = psutil.Process()

    def process_resources() -> tuple[int, int]:
        return process.memory_info().rss, process.num_threads()

    baseline_rss, baseline_threads = await anyio.to_thread.run_sync(process_resources)
    baseline_tasks = len(anyio.get_running_tasks())
    config = ResourceConfig(cpu_budget_fraction=1, cpu_memory_interval_seconds=60)
    async with ResourceManager(config) as manager:
        snapshot = await manager.snapshot()
        overhead = snapshot.overhead
        assert overhead.cpu_seconds.value is not None
        assert overhead.managed_state_bytes.value is not None
        assert 0 < overhead.managed_state_bytes.value < config.managed_state_budget_bytes
        assert overhead.sampling_duration_seconds.value is not None
        assert 0 < overhead.sampling_duration_seconds.value < config.provider_timeout_seconds
        assert overhead.provider_duration_seconds.value is not None
        assert 0 < overhead.provider_duration_seconds.value < config.provider_timeout_seconds + 1

        started = time.perf_counter()
        with anyio.fail_after(config.provider_timeout_seconds * 3 + 5):
            await manager.refresh_accelerators()
        provider_seconds = time.perf_counter() - started

        before_rss, before_threads = await anyio.to_thread.run_sync(process_resources)
        before_tasks = len(anyio.get_running_tasks())
        cached = await manager.snapshot()
        for _ in range(500):
            assert await manager.snapshot() is cached
        after_rss, after_threads = await anyio.to_thread.run_sync(process_resources)
        after_tasks = len(anyio.get_running_tasks())

        print(
            f"{anyio_backend} sampling={overhead.sampling_duration_seconds.value:.3f}s "
            f"inventory={overhead.provider_duration_seconds.value:.3f}s "
            f"accelerator refresh={provider_seconds:.3f}s "
            f"attributed CPU={overhead.cpu_seconds.value:.3f}s "
            f"managed={overhead.managed_state_bytes.value}B "
            f"RSS delta={after_rss - baseline_rss}B "
            f"thread delta={after_threads - baseline_threads} "
            f"visible task delta={after_tasks - baseline_tasks}"
        )
        assert after_rss - before_rss < 16 * 1024 * 1024
        assert after_threads - before_threads <= 2
        assert after_tasks - before_tasks <= 2


@pytest.mark.anyio
async def test_concurrent_work_never_exceeds_admission_limit(snapshot: ResourceSnapshot) -> None:
    config = ResourceConfig(
        cpu_budget_fraction=1,
        cpu_memory_interval_seconds=60,
        max_concurrency=4,
        max_waiters=32,
    )
    async with ResourceManager(config) as manager:
        assert manager._runtime is not None
        manager._runtime._snapshot = snapshot
        active = 0
        peak = 0
        completed = 0
        lock = anyio.Lock()

        async def work() -> None:
            nonlocal active, peak, completed
            async with manager.admission({"memory": "10MB"}, wait=True, wait_timeout_seconds=10):
                async with lock:
                    active += 1
                    peak = max(peak, active)
                await anyio.sleep(0.01)
                async with lock:
                    active -= 1
                    completed += 1

        async with anyio.create_task_group() as tasks:
            for _ in range(20):
                tasks.start_soon(work)
        assert completed == 20
        assert 1 <= peak <= config.max_concurrency
        assert (await manager.capacity({"memory": "10MB"})).max_concurrency == 4
