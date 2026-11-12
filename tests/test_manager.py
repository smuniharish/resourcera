import threading
import time

import anyio
import psutil
import pytest
from refresh_engine import InMemoryStateStore

from resourcera import ManagerStateError, ResourceConfig, ResourceManager, UnsupportedScopeError
from resourcera.inventory import InventoryReport
from resourcera.models import DeviceStatus, MetricStatus, Scope
from resourcera.telemetry import SystemSampler


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture
def inventory_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    async def probe(config: ResourceConfig) -> InventoryReport:
        return InventoryReport(items={"cpu": {"logical_cores": 2}}, complete=True)

    monkeypatch.setattr("resourcera.inventory.probe_inventory", probe)


@pytest.mark.anyio
async def test_lifecycle_and_cached_snapshots(inventory_probe: None) -> None:
    manager = ResourceManager(ResourceConfig(cpu_memory_interval_seconds=60))
    with pytest.raises(ManagerStateError):
        await manager.snapshot()
    async with manager:
        first = await manager.snapshot()
        assert first.host_id
        assert first.process_id
        assert first.scope is Scope.MACHINE
        assert await manager.snapshot() is first
        await manager.start()
    await manager.close()
    with pytest.raises(ManagerStateError):
        await manager.snapshot()
    with pytest.raises(ManagerStateError):
        await manager.start()


@pytest.mark.anyio
async def test_start_without_cpu_frequency(
    inventory_probe: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delattr(psutil, "cpu_freq", raising=False)
    async with ResourceManager(ResourceConfig(cpu_budget_fraction=1)) as manager:
        frequency = (await manager.snapshot()).cpu.frequency_mhz
        assert frequency.value is None
        assert frequency.status is MetricStatus.UNSUPPORTED


@pytest.mark.anyio
async def test_slow_sampling_does_not_block_application_loop(
    inventory_probe: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = SystemSampler.sample

    def slow(self: SystemSampler, *, sample_io: bool = True):
        time.sleep(0.15)
        return original(self, sample_io=sample_io)

    monkeypatch.setattr(SystemSampler, "sample", slow)
    manager = ResourceManager()
    ticked = anyio.Event()

    async def tick() -> None:
        await anyio.sleep(0.02)
        ticked.set()

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(tick)
        await manager.start()
        assert ticked.is_set()
        await manager.close()


@pytest.mark.anyio
async def test_sampling_failure_remains_visible(
    inventory_probe: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = SystemSampler.sample
    calls = 0

    def failing(self: SystemSampler, *, sample_io: bool = True):
        nonlocal calls
        calls += 1
        if calls > 1:
            raise OSError("probe unavailable")
        return original(self, sample_io=sample_io)

    monkeypatch.setattr(SystemSampler, "sample", failing)
    config = ResourceConfig(cpu_memory_interval_seconds=0.05)
    async with ResourceManager(config) as manager:
        with anyio.fail_after(2):
            while True:
                snapshot = await manager.snapshot()
                provider = next(item for item in snapshot.providers if item.name == "psutil")
                if DeviceStatus.PROVIDER_ERROR in provider.statuses:
                    break
                await anyio.sleep(0.02)
        assert "probe unavailable" in (provider.detail or "")


@pytest.mark.anyio
async def test_cancellation_during_start_cleans_up(monkeypatch: pytest.MonkeyPatch) -> None:
    async def stuck(config: ResourceConfig) -> InventoryReport:
        await anyio.sleep_forever()
        raise AssertionError("unreachable")

    monkeypatch.setattr("resourcera.inventory.probe_inventory", stuck)
    manager = ResourceManager()
    with anyio.move_on_after(0.1) as scope:
        await manager.start()
    assert scope.cancel_called
    await manager.close()
    with pytest.raises(ManagerStateError):
        await manager.snapshot()


@pytest.mark.anyio
async def test_timed_out_sampling_does_not_spawn_replacement_workers(
    inventory_probe: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = SystemSampler.sample
    release = threading.Event()
    calls = 0

    def blocked(self: SystemSampler, *, sample_io: bool = True):
        nonlocal calls
        calls += 1
        if calls > 1:
            release.wait(5)
        return original(self, sample_io=sample_io)

    monkeypatch.setattr(SystemSampler, "sample", blocked)
    config = ResourceConfig(cpu_memory_interval_seconds=0.05, provider_timeout_seconds=0.2)
    manager = ResourceManager(config)
    try:
        await manager.start()
        await anyio.sleep(0.6)
        snapshot = await manager.snapshot()
        provider = next(item for item in snapshot.providers if item.name == "psutil")
        assert DeviceStatus.PROVIDER_ERROR in provider.statuses
        assert calls == 2
        with anyio.fail_after(1):
            await manager.close()
    finally:
        release.set()
        await manager.close()


def test_distributed_scope_is_explicitly_unsupported() -> None:
    with pytest.raises(UnsupportedScopeError):
        ResourceManager(scope="distributed")


@pytest.mark.anyio
async def test_user_inventory_store_is_used(inventory_probe: None) -> None:
    store = InMemoryStateStore()
    async with ResourceManager(inventory_store=store):
        state = await store.load_all()
        assert "cpu" in state
