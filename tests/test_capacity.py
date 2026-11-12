import time

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from resourcera import (
    DeviceReading,
    DeviceStatus,
    MetricStatus,
    ProviderInfo,
    ResourceConfig,
    ResourceManager,
    WorkloadRequirements,
)
from resourcera.accelerators import normalize_device
from resourcera.capacity import ResourceClaims, estimate_capacity
from resourcera.metrics import SampleClock, metric
from resourcera.models import LimitState, ResourceSnapshot, Scope


def test_cpu_quota_memory_limits_claims_and_margin_are_combined(snapshot: ResourceSnapshot) -> None:
    requirements = WorkloadRequirements(cpu=0.5, memory="1GB")
    claims = ResourceClaims(cpu=0.35, memory_bytes=300_000_000)
    result = estimate_capacity(snapshot, ResourceConfig(), requirements, claims)
    assert result.available["cpu"].value == pytest.approx(1.0)
    assert result.available["memory"].value == pytest.approx(2_400_000_000)
    assert result.max_concurrency == 2
    assert result.limiting_resource == "cpu"
    assert any("overlap" in assumption for assumption in result.assumptions)


def test_stale_and_unknown_required_metrics_do_not_become_zero(snapshot: ResourceSnapshot) -> None:
    stale = snapshot.cpu.per_core_utilization_percent.model_copy(
        update={"sampled_monotonic": time.monotonic() - 100, "fresh_for_seconds": 1}
    )
    cpu = snapshot.cpu.model_copy(update={"per_core_utilization_percent": stale})
    result = estimate_capacity(
        snapshot.model_copy(update={"cpu": cpu}),
        ResourceConfig(),
        WorkloadRequirements(cpu=1),
    )
    assert result.max_concurrency is None
    assert result.limiting_resource == "cpu"
    assert result.available["cpu"].value is None
    assert result.available["cpu"].status is MetricStatus.STALE
    assert result.available["cpu"].unit == "CPU equivalents"

    unknown = snapshot.memory.model_copy(update={"cgroup_limit_state": LimitState.UNKNOWN})
    result = estimate_capacity(
        snapshot.model_copy(update={"memory": unknown}),
        ResourceConfig(),
        WorkloadRequirements(memory="1GB"),
    )
    assert result.max_concurrency is None
    assert result.available["memory"].value is None


def test_failed_system_sampler_blocks_cached_capacity(snapshot: ResourceSnapshot) -> None:
    failed = ProviderInfo(
        name="psutil",
        statuses=(DeviceStatus.PROVIDER_ERROR,),
        detail="sampling failed",
    )
    snapshot = snapshot.model_copy(update={"providers": (*snapshot.providers, failed)})
    result = estimate_capacity(snapshot, ResourceConfig(), WorkloadRequirements(memory="1GB"))
    assert result.max_concurrency is None
    assert result.available["memory"].status is MetricStatus.ERROR
    assert "sampling failed" in result.available["memory"].reason


def test_measured_zero_is_known_insufficient_capacity(snapshot: ResourceSnapshot) -> None:
    clock = SampleClock.now()
    empty = metric(0, "bytes", "test", Scope.MACHINE, clock, 60)
    memory = snapshot.memory.model_copy(update={"cgroup_available_bytes": empty})
    result = estimate_capacity(
        snapshot.model_copy(update={"memory": memory}),
        ResourceConfig(),
        WorkloadRequirements(memory="1GB"),
    )
    assert result.available["memory"].value == 0
    assert result.max_concurrency == 0


def test_disk_and_network_need_real_ceilings(snapshot: ResourceSnapshot) -> None:
    requirements = WorkloadRequirements(
        disk_read_bytes_per_second=20, disk_write_bytes_per_second=10, network_bytes_per_second=10
    )
    unknown = estimate_capacity(snapshot, ResourceConfig(), requirements)
    assert unknown.max_concurrency is None
    assert unknown.available["disk_read"].value is None
    config = ResourceConfig(
        disk_read_capacity_bytes_per_second=100,
        disk_write_capacity_bytes_per_second=100,
        network_capacity_bytes_per_second=100,
        safety_margin=0,
    )
    claims = ResourceClaims(disk_read_bytes_per_second=20, network_bytes_per_second=10)
    result = estimate_capacity(snapshot, config, requirements, claims)
    assert result.available["disk_read"].value == 70
    assert result.available["disk_write"].value == 80
    assert result.available["network"].value == 80
    assert result.max_concurrency == 3
    assert result.limiting_resource == "disk_read"


def test_accelerator_memory_is_per_device_and_claims_are_exclusive(
    snapshot: ResourceSnapshot,
) -> None:
    clock = SampleClock.now()
    readings = (
        DeviceReading(
            id="gpu-1",
            vendor="NVIDIA",
            kind="gpu",
            source="test",
            statuses=(DeviceStatus.TELEMETRY_AVAILABLE,),
            memory_total_bytes=8_000_000_000,
            memory_used_bytes=1_000_000_000,
        ),
        DeviceReading(
            id="gpu-2",
            vendor="NVIDIA",
            kind="gpu",
            source="test",
            statuses=(DeviceStatus.TELEMETRY_AVAILABLE,),
            memory_total_bytes=8_000_000_000,
            memory_used_bytes=6_000_000_000,
        ),
    )
    devices = tuple(normalize_device(reading, clock, 60) for reading in readings)
    snapshot = snapshot.model_copy(update={"accelerators": devices})
    request = WorkloadRequirements(
        accelerator_type="gpu", accelerator_memory="4GB", accelerator_count=2
    )
    result = estimate_capacity(snapshot, ResourceConfig(), request)
    assert result.accelerator_devices == ("gpu-1",)
    assert result.max_concurrency == 0

    request = request.model_copy(update={"accelerator_count": 1})
    result = estimate_capacity(snapshot, ResourceConfig(), request)
    assert result.max_concurrency == 1
    claimed = ResourceClaims(accelerator_devices=frozenset({"gpu-1"}))
    result = estimate_capacity(snapshot, ResourceConfig(), request, claimed)
    assert result.max_concurrency == 0


def test_partially_unknown_multi_device_capacity_is_not_reported_as_zero(
    snapshot: ResourceSnapshot,
) -> None:
    clock = SampleClock.now()
    known = normalize_device(
        DeviceReading(
            id="known",
            vendor="NVIDIA",
            kind="gpu",
            source="test",
            statuses=(DeviceStatus.TELEMETRY_AVAILABLE,),
            memory_total_bytes=8_000_000_000,
            memory_used_bytes=1_000_000_000,
        ),
        clock,
        60,
    )
    unknown = normalize_device(
        DeviceReading(
            id="unknown",
            vendor="NVIDIA",
            kind="gpu",
            source="test",
            statuses=(DeviceStatus.PROVIDER_ERROR,),
        ),
        clock,
        60,
    )
    snapshot = snapshot.model_copy(update={"accelerators": (known, unknown)})
    two = WorkloadRequirements(
        accelerator_type="gpu", accelerator_memory="4GB", accelerator_count=2
    )
    result = estimate_capacity(snapshot, ResourceConfig(), two)
    assert result.available["accelerators"].value is None
    assert result.max_concurrency is None
    one = two.model_copy(update={"accelerator_count": 1})
    result = estimate_capacity(snapshot, ResourceConfig(), one)
    assert result.max_concurrency == 1
    assert result.accelerator_devices == ("known",)


def test_pressure_and_throttling_reduce_cpu_headroom(snapshot: ResourceSnapshot) -> None:
    clock = SampleClock.now()
    throttle = metric(0.25, "seconds/second", "test", Scope.PROCESS, clock, 60)
    cpu = snapshot.cpu.model_copy(update={"throttled_seconds_per_second": throttle})
    pressure = snapshot.pressure.model_copy(
        update={"cpu_percent": metric(20.0, "percent", "test", Scope.MACHINE, clock, 60)}
    )
    result = estimate_capacity(
        snapshot.model_copy(update={"cpu": cpu, "pressure": pressure}),
        ResourceConfig(safety_margin=0),
        WorkloadRequirements(cpu=0.5, profile="custom-training"),
    )
    assert result.available["cpu"].value == pytest.approx(0.9)
    assert result.max_concurrency == 1


@settings(max_examples=40, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    lower=st.floats(min_value=0, max_value=0.8, allow_nan=False, allow_infinity=False),
    upper=st.floats(min_value=0, max_value=0.8, allow_nan=False, allow_infinity=False),
)
def test_increasing_safety_margin_never_increases_capacity(
    snapshot: ResourceSnapshot, lower: float, upper: float
) -> None:
    low, high = sorted((lower, upper))
    config = ResourceConfig()
    request = WorkloadRequirements(cpu=0.1, memory="100MB", safety_margin=low)
    generous = estimate_capacity(snapshot, config, request)
    cautious = estimate_capacity(
        snapshot, config, request.model_copy(update={"safety_margin": high})
    )
    assert generous.max_concurrency is not None
    assert cautious.max_concurrency is not None
    assert cautious.max_concurrency <= generous.max_concurrency


@pytest.mark.anyio
async def test_public_capacity_uses_cached_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    async with ResourceManager() as manager:
        first = await manager.snapshot()
        result = await manager.capacity({"memory": "1MB"})
        assert result.snapshot_id == first.id
        assert result.available["memory"].unit == "bytes"
