import anyio
import pytest

from resourcera import DeviceStatus, ProviderInfo, ResourceManager
from resourcera.metrics import SampleClock, metric
from resourcera.models import LimitState, NetworkInterfaceMetrics, ResourceSnapshot, Scope


@pytest.fixture(scope="session")
def snapshot_template() -> ResourceSnapshot:
    async def sample() -> ResourceSnapshot:
        async with ResourceManager() as manager:
            return await manager.snapshot()

    return anyio.run(sample)


@pytest.fixture
def snapshot(snapshot_template: ResourceSnapshot) -> ResourceSnapshot:
    clock = SampleClock.now()

    def measured[T](value: T, unit: str):
        return metric(value, unit, "test", Scope.MACHINE, clock, 60)

    cpu = snapshot_template.cpu.model_copy(
        update={
            "logical_cores": measured(8, "cores"),
            "affinity_cpus": measured((0, 1, 2, 3), "cpu indices"),
            "per_core_utilization_percent": measured(
                (50.0, 50.0, 100.0, 0.0, 0.0, 0.0, 0.0, 0.0), "percent"
            ),
            "utilization_percent": measured(25.0, "percent"),
            "quota_cores": measured(2.0, "CPU equivalents"),
            "quota_state": LimitState.FINITE,
            "cgroup_utilization_cores": measured(0.5, "CPU equivalents"),
            "cgroup_headroom_cores": measured(1.5, "CPU equivalents"),
            "throttled_seconds_per_second": measured(0.0, "throttled seconds/second"),
        }
    )
    memory = snapshot_template.memory.model_copy(
        update={
            "available_bytes": measured(8_000_000_000, "bytes"),
            "cgroup_limit_bytes": measured(4_000_000_000, "bytes"),
            "cgroup_limit_state": LimitState.FINITE,
            "cgroup_available_bytes": measured(3_000_000_000, "bytes"),
        }
    )
    pressure = snapshot_template.pressure.model_copy(
        update={
            "cpu_percent": measured(0.0, "percent"),
            "memory_percent": measured(0.0, "percent"),
            "io_percent": measured(0.0, "percent"),
        }
    )
    disk = snapshot_template.disk.model_copy(
        update={
            "read_bytes_per_second": measured(10.0, "bytes/second"),
            "write_bytes_per_second": measured(20.0, "bytes/second"),
        }
    )
    network = NetworkInterfaceMetrics(
        name="eth0",
        link_speed_bps=measured(1_000.0, "bits/second"),
        sent_bytes_per_second=measured(5.0, "bytes/second"),
        received_bytes_per_second=measured(5.0, "bytes/second"),
        sent_packets_per_second=measured(1.0, "packets/second"),
        received_packets_per_second=measured(1.0, "packets/second"),
        errors=measured(0, "errors"),
        drops=measured(0, "packets"),
    )
    provider = ProviderInfo(
        name="operating-system accelerator discovery",
        statuses=(DeviceStatus.PROVIDER_AVAILABLE,),
    )
    return snapshot_template.model_copy(
        update={
            "cpu": cpu,
            "memory": memory,
            "disk": disk,
            "network": (network,),
            "pressure": pressure,
            "accelerators": (),
            "providers": (provider,),
        }
    )
