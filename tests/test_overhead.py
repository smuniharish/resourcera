import threading
import time

import anyio
import psutil
import pytest

from resourcera import AdmissionRejectedError, DeviceReading, OverheadBudgetError, ResourceManager
from resourcera.accelerators import PlatformAccelerators, normalize_device
from resourcera.config import ResourceConfig
from resourcera.inventory import InventoryReport
from resourcera.metrics import SampleClock
from resourcera.models import DeviceStatus, MetricStatus
from resourcera.overhead import ManagedSize, OverheadController, OverheadState, managed_size


def test_cpu_thresholds_and_recovery_hysteresis() -> None:
    control = OverheadController(
        ResourceConfig(cpu_budget_fraction=0.01, cpu_budget_window_seconds=100)
    )
    control.record_cpu(0.8, 10)
    assert control.state is OverheadState.REDUCED
    control.record_cpu(0.2, 10)
    assert control.state is OverheadState.SUSPENDED
    assert control.evaluate(111.7) is OverheadState.NORMAL
    assert control.cpu_seconds(111.7) == 0


def test_memory_thresholds_and_incomplete_estimates_fail_closed() -> None:
    control = OverheadController(ResourceConfig(managed_state_budget_bytes=1000))
    control.set_managed_size(ManagedSize(800, True), 0)
    assert control.state is OverheadState.REDUCED
    control.set_managed_size(ManagedSize(1000, True), 1)
    assert control.state is OverheadState.SUSPENDED
    control.set_managed_size(ManagedSize(700, True), 2)
    assert control.state is OverheadState.SUSPENDED
    control.set_managed_size(ManagedSize(699, True), 3)
    assert control.state is OverheadState.REDUCED
    control.set_managed_size(ManagedSize(599, True), 4)
    assert control.state is OverheadState.NORMAL
    control.set_managed_size(ManagedSize(1, False), 5)
    assert control.state is OverheadState.SUSPENDED


def test_managed_size_avoids_cycles_and_has_a_bounded_traversal() -> None:
    values: list[object] = []
    values.append(values)
    assert managed_size(values).complete
    assert not managed_size(list(range(100)), max_objects=5).complete


def test_provider_costs_preserve_unknowns() -> None:
    control = OverheadController(ResourceConfig())
    control.record_provider(None, 0.1, 0, accelerator=True)
    assert control.last_provider_cpu is None
    assert control.last_accelerator_cpu is None
    assert control.unmeasured_provider_work


def test_bootstrap_allowance_is_bounded_and_still_reported() -> None:
    control = OverheadController(
        ResourceConfig(cpu_budget_fraction=0.01, cpu_budget_window_seconds=100)
    )
    control.record_cpu(1.2, 1, bootstrap=True)
    assert control.bootstrap_seconds(1) == 1
    assert control.cpu_seconds(1) == 1.2
    assert control.state is OverheadState.NORMAL
    control.record_cpu(0.8, 2, bootstrap=True)
    assert control.bootstrap_seconds(2) == 1
    assert control.state is OverheadState.SUSPENDED


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.mark.anyio
async def test_budget_exhaustion_refuses_new_optional_probes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    platform = PlatformAccelerators(devices=(), complete=True, nvidia_installed=True)

    async def probe(config: ResourceConfig) -> InventoryReport:
        return InventoryReport(
            items={"accelerators": platform.model_dump(mode="json")},
            complete=True,
            cpu_seconds=2,
        )

    calls = 0

    async def should_not_run(*args: object) -> None:
        nonlocal calls
        calls += 1

    monkeypatch.setattr("resourcera.inventory.probe_inventory", probe)
    monkeypatch.setattr("resourcera.manager.run_provider", should_not_run)
    config = ResourceConfig(cpu_budget_fraction=0.01, cpu_budget_window_seconds=100)
    async with ResourceManager(config) as manager:
        snapshot = await manager.snapshot()
        assert snapshot.overhead.state == "suspended"
        assert snapshot.overhead.cpu_seconds.value is not None
        assert snapshot.overhead.bootstrap_cpu_seconds.value == 1
        with pytest.raises(OverheadBudgetError):
            await manager.refresh_accelerators()
        assert calls == 0


@pytest.mark.anyio
async def test_managed_state_pressure_evicts_optional_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    platform = PlatformAccelerators(devices=(), complete=True, nvidia_installed=False)

    async def probe(config: ResourceConfig) -> InventoryReport:
        return InventoryReport(
            items={
                "accelerators": platform.model_dump(mode="json"),
                "interfaces": ["x" * 100_000],
            },
            complete=True,
        )

    monkeypatch.setattr("resourcera.inventory.probe_inventory", probe)
    config = ResourceConfig(managed_state_budget_bytes=50_000)
    async with ResourceManager(config) as manager:
        snapshot = await manager.snapshot()
        assert snapshot.overhead.degraded
        assert snapshot.network == ()
        assert snapshot.disk.filesystems == ()
        assert snapshot.accelerators == ()
        assert any(
            DeviceStatus.PARTIAL_TELEMETRY in provider.statuses
            and "evicted" in (provider.detail or "")
            for provider in snapshot.providers
        )


@pytest.mark.anyio
async def test_suspended_policy_stales_accelerators_and_rejects_new_work() -> None:
    config = ResourceConfig(cpu_budget_fraction=0.01, cpu_budget_window_seconds=100)
    async with ResourceManager(config) as manager:
        reading = DeviceReading(
            id="device",
            vendor="NVIDIA",
            kind="gpu",
            source="nvidia-nvml",
            statuses=(DeviceStatus.TELEMETRY_AVAILABLE,),
            memory_total_bytes=8_000_000_000,
            memory_used_bytes=1_000_000_000,
        )
        device = normalize_device(reading, SampleClock.now(), 60)

        async def suspend(runtime):
            runtime._accelerators = (device,)
            runtime._snapshot = runtime.snapshot().model_copy(update={"accelerators": (device,)})
            runtime._overhead.record_cpu(5, time.monotonic())
            await runtime._apply_overhead_policy()

        await manager._call_runtime(suspend)
        snapshot = await manager.snapshot()
        assert snapshot.accelerators[0].memory_total_bytes.status is MetricStatus.STALE
        assert DeviceStatus.PARTIAL_TELEMETRY in snapshot.accelerators[0].statuses
        with pytest.raises(AdmissionRejectedError, match="self-overhead"):
            await manager.reserve({"memory": "1MB"})


@pytest.mark.anyio
async def test_suspended_policy_stales_process_connection_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(psutil.Process, "net_connections", lambda self, kind: [1, 2])
    config = ResourceConfig(sample_process_connections=True, cpu_budget_fraction=1)
    async with ResourceManager(config) as manager:
        assert (await manager.snapshot()).network_connections.value == 2

        async def suspend(runtime):
            runtime._overhead.record_cpu(120, time.monotonic())
            cached = runtime._sampler._connections
            assert runtime._sample_gate.acquire(blocking=False)
            try:
                await runtime._apply_overhead_policy()
                assert runtime.snapshot().network_connections.status is MetricStatus.STALE
                assert runtime._sampler._connections is cached
            finally:
                runtime._sample_gate.release()

            monkeypatch.setattr(
                runtime, "_effective_overhead_state", lambda now=None: OverheadState.NORMAL
            )
            cached_sample = await anyio.to_thread.run_sync(runtime._sample_blocking, False)
            runtime._publish(cached_sample)
            assert runtime.snapshot().network_connections.status is MetricStatus.STALE
            fresh_sample = await anyio.to_thread.run_sync(runtime._sample_blocking, True)
            runtime._publish(fresh_sample)
            assert runtime.snapshot().network_connections.status is MetricStatus.ESTIMATED

        await manager._call_runtime(suspend)
        reading = (await manager.snapshot()).network_connections
        assert reading.value == 2
        assert reading.status is MetricStatus.ESTIMATED


@pytest.mark.anyio
async def test_slow_connection_probe_does_not_block_loop_or_spawn_replacements(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = ResourceConfig(
        sample_process_connections=True,
        cpu_memory_interval_seconds=0.1,
        disk_network_interval_seconds=0.1,
        provider_timeout_seconds=2,
        cpu_budget_fraction=1,
    )
    async with ResourceManager(config) as manager:
        entered = threading.Event()
        release = threading.Event()
        calls = 0

        def slow(self: psutil.Process, kind: str) -> list[object]:
            nonlocal calls
            calls += 1
            entered.set()
            release.wait(8)
            return []

        monkeypatch.setattr(psutil.Process, "net_connections", slow)
        try:
            with anyio.fail_after(6):
                assert await anyio.to_thread.run_sync(entered.wait, 3)
                while True:
                    snapshot = await manager.snapshot()
                    if any(
                        provider.name == "psutil"
                        and DeviceStatus.PROVIDER_ERROR in provider.statuses
                        for provider in snapshot.providers
                    ):
                        break
                    await anyio.sleep(0.02)
                await anyio.sleep(0.3)
                assert calls == 1
        finally:
            release.set()
