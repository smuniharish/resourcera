import sys
from collections import namedtuple
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import psutil
import pytest

from resourcera.cgroups import CgroupLimit, CgroupMeasurements, CpuQuotaDomain
from resourcera.config import ResourceConfig
from resourcera.metrics import SampleClock
from resourcera.models import LimitState, MetricStatus
from resourcera.telemetry import SystemSampler, _cpu_percent, _rate

CpuTimes = namedtuple("CpuTimes", "user system idle")


def test_counter_deltas_reject_warmup_and_resets() -> None:
    assert _cpu_percent(None, {"user": 1.0, "idle": 9.0}) is None
    assert _cpu_percent({"user": 1.0, "idle": 9.0}, {"user": 3.0, "idle": 11.0}) == 50
    assert _cpu_percent({"user": 10.0, "idle": 9.0}, {"user": 1.0, "idle": 11.0}) is None
    assert _rate(10, 5, 1) is None
    assert _rate(10, 20, 2) == 5


def test_disk_latency_uses_valid_counter_deltas(monkeypatch: pytest.MonkeyPatch) -> None:
    _isolate_optional_probes(monkeypatch)
    counters = SimpleNamespace(
        read_bytes=1000,
        write_bytes=500,
        read_count=100,
        write_count=50,
        read_time=500,
        write_time=200,
    )
    monkeypatch.setattr(psutil, "disk_io_counters", lambda: counters)
    sampler = SystemSampler(ResourceConfig())

    def sample(at: float):
        return sampler._sample_io(SampleClock(datetime.now(UTC), at), optional=False)[0].latency_ms

    assert sample(100).measurement_status is MetricStatus.UNAVAILABLE
    counters.read_count = 110
    counters.write_count = 55
    counters.read_time = 520
    counters.write_time = 230
    latency = sample(101)
    assert latency.value == pytest.approx(50 / 15)
    assert latency.measurement_status is MetricStatus.ESTIMATED
    assert latency.unit == "milliseconds"
    assert latency.source == "psutil.disk_io_counters"

    idle = sample(102)
    assert idle.value is None
    assert idle.measurement_status is MetricStatus.UNAVAILABLE

    counters.read_count += 1
    unmeasured = sample(103)
    assert unmeasured.value is None
    assert unmeasured.measurement_status is MetricStatus.UNAVAILABLE

    counters.read_count = 1
    counters.read_time = 1
    reset = sample(104)
    assert reset.value is None
    assert reset.measurement_status is MetricStatus.UNAVAILABLE

    del counters.read_time
    unsupported = sample(105)
    assert unsupported.value is None
    assert unsupported.measurement_status is MetricStatus.UNSUPPORTED

    counters.read_time = 10
    assert sample(106).measurement_status is MetricStatus.UNAVAILABLE

    def failed() -> None:
        raise OSError("disk counters failed")

    monkeypatch.setattr(psutil, "disk_io_counters", failed)
    error = sample(107)
    assert error.value is None
    assert error.measurement_status is MetricStatus.ERROR
    assert "disk counters failed" in (error.detail or "")


def _isolate_optional_probes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(psutil, "disk_partitions", lambda all=False: [])
    monkeypatch.setattr(psutil, "disk_io_counters", lambda: None)
    monkeypatch.setattr(psutil, "net_io_counters", lambda pernic=False: {})
    monkeypatch.setattr(psutil, "net_if_stats", lambda: {})
    monkeypatch.setattr(psutil, "cpu_freq", lambda: None)
    limit = CgroupLimit(LimitState.NOT_APPLICABLE)
    monkeypatch.setattr(
        "resourcera.telemetry.read_cgroup", lambda: CgroupMeasurements(limit, limit)
    )


def test_sampler_requires_two_cpu_observations(monkeypatch: pytest.MonkeyPatch) -> None:
    _isolate_optional_probes(monkeypatch)
    state = {"value": CpuTimes(10.0, 0.0, 90.0)}

    def cpu_times(percpu: bool = False) -> CpuTimes | list[CpuTimes]:
        return [state["value"]] if percpu else state["value"]

    monkeypatch.setattr(psutil, "cpu_times", cpu_times)
    sampler = SystemSampler(ResourceConfig())
    first = sampler.sample(sample_io=False)
    state["value"] = CpuTimes(15.0, 0.0, 95.0)
    second = sampler.sample(sample_io=False)

    assert first.cpu.utilization_percent.status is MetricStatus.UNAVAILABLE
    assert first.cpu.utilization_percent.value is None
    assert second.cpu.utilization_percent.value == 50
    assert second.cpu.per_core_utilization_percent.value == (50,)


def test_sampler_reports_cpu_probe_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _isolate_optional_probes(monkeypatch)

    def fail(percpu: bool = False) -> None:
        raise OSError("CPU probe failed")

    monkeypatch.setattr(psutil, "cpu_times", fail)
    sampled = SystemSampler(ResourceConfig()).sample()
    assert sampled.cpu.utilization_percent.status is MetricStatus.ERROR
    assert sampled.cpu.utilization_percent.value is None
    assert "CPU probe failed" in (sampled.cpu.utilization_percent.detail or "")


def test_process_virtual_memory_preserves_availability(monkeypatch: pytest.MonkeyPatch) -> None:
    _isolate_optional_probes(monkeypatch)
    sampler = SystemSampler(ResourceConfig())
    monkeypatch.setattr(sampler._process, "memory_info", lambda: SimpleNamespace(rss=123, vms=456))
    memory = sampler.sample().memory
    available = memory.process_vms_bytes
    assert (available.value, available.unit, available.status) == (
        456,
        "bytes",
        MetricStatus.AVAILABLE,
    )
    if sys.platform == "win32":
        assert "PagefileUsage" in (available.detail or "")
        assert not memory.process_rss_bytes.detail

    def denied() -> None:
        raise psutil.AccessDenied(pid=1)

    monkeypatch.setattr(sampler._process, "memory_info", denied)
    unavailable = sampler.sample().memory.process_vms_bytes
    assert unavailable.value is None
    assert unavailable.status is MetricStatus.UNAVAILABLE


def test_linux_runnable_task_count_and_malformed_input(monkeypatch: pytest.MonkeyPatch) -> None:
    from resourcera.telemetry import _runnable_tasks

    monkeypatch.setattr("resourcera.telemetry.sys.platform", "linux")
    monkeypatch.setattr(Path, "read_text", lambda *args, **kwargs: "0.1 0.2 0.3 2/100 123\n")
    reading = _runnable_tasks(SampleClock.now(), 2)
    assert (reading.value, reading.unit, reading.status) == (2, "tasks", MetricStatus.AVAILABLE)

    monkeypatch.setattr(Path, "read_text", lambda *args, **kwargs: "invalid")
    invalid = _runnable_tasks(SampleClock.now(), 2)
    assert invalid.value is None
    assert invalid.status is MetricStatus.ERROR

    def denied(*args: object, **kwargs: object) -> str:
        raise PermissionError("not readable")

    monkeypatch.setattr(Path, "read_text", denied)
    unavailable = _runnable_tasks(SampleClock.now(), 2)
    assert unavailable.value is None
    assert unavailable.status is MetricStatus.UNAVAILABLE

    monkeypatch.setattr("resourcera.telemetry.sys.platform", "win32")
    unsupported = _runnable_tasks(SampleClock.now(), 2)
    assert unsupported.value is None
    assert unsupported.status is MetricStatus.UNSUPPORTED


def test_process_connection_count_is_opt_in_and_preserves_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _isolate_optional_probes(monkeypatch)
    disabled = SystemSampler(ResourceConfig())

    def unexpected(*args: object, **kwargs: object) -> None:
        raise AssertionError("disabled connection sampling must not run")

    monkeypatch.setattr(disabled._process, "net_connections", unexpected)
    disabled_reading = disabled.sample().network_connections
    assert disabled_reading.value is None
    assert disabled_reading.status is MetricStatus.UNAVAILABLE

    enabled = SystemSampler(ResourceConfig(sample_process_connections=True))
    monkeypatch.setattr(enabled._process, "net_connections", lambda **kwargs: [1, 2])
    reading = enabled.sample().network_connections
    assert (reading.value, reading.unit, reading.status) == (
        2,
        "connections",
        MetricStatus.ESTIMATED,
    )

    def denied(**kwargs: object) -> None:
        raise psutil.AccessDenied(pid=1)

    monkeypatch.setattr(enabled._process, "net_connections", denied)
    unavailable = enabled.sample().network_connections
    assert unavailable.value is None
    assert unavailable.status is MetricStatus.UNAVAILABLE


def test_connection_count_is_not_probed_during_paused_initial_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _isolate_optional_probes(monkeypatch)
    sampler = SystemSampler(ResourceConfig(sample_process_connections=True))

    def unexpected(**kwargs: object) -> None:
        raise AssertionError("paused optional connection sampling must not run")

    monkeypatch.setattr(sampler._process, "net_connections", unexpected)
    reading = sampler.sample(sample_io=False).network_connections
    assert reading.value is None
    assert reading.status is MetricStatus.UNAVAILABLE


def test_ancestor_quota_headroom_includes_sibling_usage(monkeypatch: pytest.MonkeyPatch) -> None:
    _isolate_optional_probes(monkeypatch)
    clock_values = iter(
        [
            SampleClock(datetime.now(UTC), 100),
            SampleClock(datetime.now(UTC), 101),
            SampleClock(datetime.now(UTC), 102),
        ]
    )
    readings = iter(
        [
            (10.0, 20.0),
            (10.1, 21.8),
            (10.2, 21.7),
        ]
    )

    def cgroup() -> CgroupMeasurements:
        child, parent = next(readings)
        return CgroupMeasurements(
            CgroupLimit(LimitState.FINITE, 2),
            CgroupLimit(LimitState.NOT_APPLICABLE),
            cpu_usage_seconds=child,
            cpu_quota_domains=(
                CpuQuotaDomain("child", 4, child),
                CpuQuotaDomain("parent", 2, parent),
            ),
        )

    monkeypatch.setattr("resourcera.telemetry.read_cgroup", cgroup)
    monkeypatch.setattr(SampleClock, "now", lambda: next(clock_values))
    sampler = SystemSampler(ResourceConfig())
    first = sampler.sample(sample_io=False)
    second = sampler.sample(sample_io=False)
    reset = sampler.sample(sample_io=False)
    assert first.cpu.cgroup_headroom_cores.value is None
    assert second.cpu.cgroup_headroom_cores.value == pytest.approx(0.2)
    assert second.cpu.cgroup_utilization_cores.value == pytest.approx(0.1)
    assert reset.cpu.cgroup_headroom_cores.value is None
