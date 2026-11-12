"""Normalize psutil observations into resource metrics."""

from __future__ import annotations

import os
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

import psutil

from resourcera.cgroups import CgroupLimit, read_cgroup
from resourcera.config import ResourceConfig
from resourcera.metrics import SampleClock, metric
from resourcera.models import (
    CpuMetrics,
    DiskMetrics,
    FilesystemMetrics,
    LimitState,
    MemoryMetrics,
    Metric,
    MetricStatus,
    NetworkInterfaceMetrics,
    PressureMetrics,
    Scope,
)


@dataclass(frozen=True, slots=True)
class _Reading[T]:
    value: T | None
    status: MetricStatus
    detail: str | None = None


def _read[T](call: Callable[[], T]) -> _Reading[T]:
    try:
        value = call()
    except NotImplementedError as exc:
        return _Reading(None, MetricStatus.UNSUPPORTED, str(exc))
    except psutil.AccessDenied as exc:
        return _Reading(None, MetricStatus.UNAVAILABLE, str(exc))
    except (OSError, psutil.Error) as exc:
        return _Reading(None, MetricStatus.ERROR, str(exc))
    if value is None:
        return _Reading(None, MetricStatus.UNAVAILABLE)
    return _Reading(value, MetricStatus.AVAILABLE)


def _from_reading[T](
    reading: _Reading[T],
    unit: str,
    source: str,
    scope: Scope,
    clock: SampleClock,
    freshness: float,
) -> Metric[T]:
    return metric(
        reading.value,
        unit,
        source,
        scope,
        clock,
        freshness,
        status=reading.status,
        detail=reading.detail,
    )


def _cpu_total_and_busy(values: Mapping[str, float]) -> tuple[float, float]:
    total = sum(values.values()) - values.get("guest", 0.0) - values.get("guest_nice", 0.0)
    busy = total - values.get("idle", 0.0) - values.get("iowait", 0.0)
    return total, busy


def _cpu_percent(
    before: Mapping[str, float] | None, after: Mapping[str, float] | None
) -> float | None:
    if before is None or after is None:
        return None
    old_total, old_busy = _cpu_total_and_busy(before)
    new_total, new_busy = _cpu_total_and_busy(after)
    elapsed = new_total - old_total
    busy = new_busy - old_busy
    if elapsed <= 0 or busy < 0 or busy > elapsed:
        return None
    return 100.0 * busy / elapsed


def _rate(before: int | float | None, after: int | float | None, elapsed: float) -> float | None:
    if before is None or after is None or elapsed <= 0 or after < before:
        return None
    return float(after - before) / elapsed


def _limit_metric(
    limit: CgroupLimit, unit: str, clock: SampleClock, freshness: float
) -> Metric[float]:
    if limit.state is LimitState.FINITE and limit.value is not None:
        return metric(float(limit.value), unit, "cgroup", Scope.PROCESS, clock, freshness)
    status = (
        MetricStatus.UNSUPPORTED
        if limit.state is LimitState.NOT_APPLICABLE
        else MetricStatus.UNAVAILABLE
    )
    detail = limit.detail or f"cgroup limit is {limit.state.value}"
    return metric(
        None, unit, "cgroup", Scope.PROCESS, clock, freshness, status=status, detail=detail
    )


def _pressure(name: str, clock: SampleClock, freshness: float) -> Metric[float]:
    if sys.platform != "linux":
        return metric(
            None,
            "percent",
            "linux-psi",
            Scope.MACHINE,
            clock,
            freshness,
            status=MetricStatus.UNSUPPORTED,
            detail="pressure stall information is Linux-specific",
        )
    value: float | None = None
    try:
        lines = (Path("/proc/pressure") / name).read_text(encoding="utf-8").splitlines()
        line = next(line for line in lines if line.startswith("some "))
        fields = dict(part.split("=", 1) for part in line.split()[1:])
        value = float(fields["avg10"])
    except (OSError, ValueError, KeyError, StopIteration) as exc:
        return metric(
            None,
            "percent",
            "linux-psi",
            Scope.MACHINE,
            clock,
            freshness,
            status=MetricStatus.UNAVAILABLE,
            detail=str(exc),
        )
    return metric(value, "percent", "linux-psi", Scope.MACHINE, clock, freshness)


def _runnable_tasks(clock: SampleClock, freshness: float) -> Metric[int]:
    if sys.platform != "linux":
        return metric(
            None,
            "tasks",
            "linux-proc-loadavg",
            Scope.MACHINE,
            clock,
            freshness,
            status=MetricStatus.UNSUPPORTED,
            detail="instantaneous runnable tasks are not exposed by this platform",
        )
    try:
        field = Path("/proc/loadavg").read_text(encoding="ascii").split()[3]
        running, total = (int(value) for value in field.split("/", 1))
        if running < 0 or total < running:
            raise ValueError("invalid runnable task count")
        return metric(
            running,
            "tasks",
            "linux-proc-loadavg",
            Scope.MACHINE,
            clock,
            freshness,
            detail="Runnable kernel scheduling entities include processes and threads.",
        )
    except (OSError, ValueError, IndexError) as exc:
        status = (
            MetricStatus.UNSUPPORTED
            if isinstance(exc, FileNotFoundError)
            else MetricStatus.UNAVAILABLE
            if isinstance(exc, PermissionError)
            else MetricStatus.ERROR
        )
        return metric(
            None,
            "tasks",
            "linux-proc-loadavg",
            Scope.MACHINE,
            clock,
            freshness,
            status=status,
            detail=str(exc),
        )


@dataclass(frozen=True, slots=True)
class Telemetry:
    """One completed group of psutil observations."""

    clock: SampleClock
    cpu: CpuMetrics
    memory: MemoryMetrics
    disk: DiskMetrics
    network: tuple[NetworkInterfaceMetrics, ...]
    network_connections: Metric[int]
    pressure: PressureMetrics
    container_id: str | None
    duration_seconds: float
    cpu_seconds: float
    warnings: tuple[str, ...]


class SystemSampler:
    """Collect system counters and normalize valid deltas between observations."""

    def __init__(self, config: ResourceConfig) -> None:
        self._config = config
        self._process = psutil.Process()
        self._cpu_before: dict[str, float] | None = None
        self._cores_before: list[dict[str, float]] | None = None
        self._process_before: float | None = None
        self._cgroup_cpu_before: float | None = None
        self._cgroup_domains_before: dict[str, tuple[float, float | None]] = {}
        self._throttled_before: float | None = None
        self._previous_time: float | None = None
        self._disk_before: tuple[int, int, int, int] | None = None
        self._disk_times_before: tuple[int | float, int | float] | None = None
        self._network_before: dict[str, tuple[int, int, int, int]] = {}
        self._io_time: float | None = None
        self._disk: DiskMetrics | None = None
        self._network: tuple[NetworkInterfaceMetrics, ...] = ()
        self._connections: Metric[int] | None = None
        self._io_warnings: tuple[str, ...] = ()
        self._frequency: Metric[float] | None = None
        self._uss: Metric[int] | None = None

    @property
    def retained_objects(self) -> tuple[object, ...]:
        """Return owned sample caches for managed-state accounting."""
        return (
            self._cpu_before,
            self._cores_before,
            self._cgroup_domains_before,
            self._disk_before,
            self._disk_times_before,
            self._network_before,
            self._disk,
            self._network,
            self._connections,
            self._frequency,
            self._uss,
        )

    def shed_optional(self) -> None:
        """Release cached filesystem and interface details between sampling calls."""
        self._network = ()
        self._network_before = {}
        self._disk_before = None
        self._disk_times_before = None
        self._io_time = None
        if self._disk is not None:
            self._disk = self._disk.model_copy(update={"filesystems": ()})

    def sample(self, *, sample_io: bool = True) -> Telemetry:
        """Collect one sample; callers must run this blocking method off-loop."""
        started = time.perf_counter()
        cpu_started = time.thread_time()
        clock = SampleClock.now()
        freshness = self._config.cpu_memory_interval_seconds * 2
        io_freshness = self._config.disk_network_interval_seconds * 2
        do_io = sample_io or self._disk is None
        limits = read_cgroup()

        cpu_total = _read(lambda: dict(psutil.cpu_times()._asdict()))
        per_cpu = _read(lambda: [dict(item._asdict()) for item in psutil.cpu_times(percpu=True)])
        process_times = _read(self._process.cpu_times)
        logical = _read(lambda: psutil.cpu_count(logical=True))
        physical = _read(lambda: psutil.cpu_count(logical=False))
        affinity_method = getattr(self._process, "cpu_affinity", None)
        affinity = (
            _read(lambda: tuple(affinity_method()))
            if affinity_method is not None
            else _Reading(None, MetricStatus.UNSUPPORTED, "CPU affinity is not exposed")
        )
        if do_io or self._frequency is None:
            frequency = _read(psutil.cpu_freq)
            self._frequency = metric(
                float(frequency.value.current) if frequency.value is not None else None,
                "MHz",
                "psutil.cpu_freq",
                Scope.MACHINE,
                clock,
                io_freshness,
                status=frequency.status,
                detail=frequency.detail,
            )
        threads = _read(self._process.num_threads)
        load = (
            _read(lambda: os.getloadavg()[0])
            if hasattr(os, "getloadavg")
            else _Reading(None, MetricStatus.UNSUPPORTED, "load averages are unavailable")
        )

        aggregate_percent = _cpu_percent(self._cpu_before, cpu_total.value)
        core_percent: tuple[float, ...] | None = None
        if (
            self._cores_before is not None
            and per_cpu.value is not None
            and len(self._cores_before) == len(per_cpu.value)
        ):
            values = [
                _cpu_percent(before, after)
                for before, after in zip(self._cores_before, per_cpu.value, strict=True)
            ]
            if all(value is not None for value in values):
                core_percent = tuple(value for value in values if value is not None)
        process_cpu = (
            process_times.value.user + process_times.value.system
            if process_times.value is not None
            else None
        )
        elapsed = clock.monotonic - self._previous_time if self._previous_time is not None else 0
        process_percent = _rate(self._process_before, process_cpu, elapsed)
        if process_percent is not None:
            process_percent *= 100
        cgroup_cpu = _rate(self._cgroup_cpu_before, limits.cpu_usage_seconds, elapsed)
        quota_headroom: float | None = None
        if limits.cpu.state is LimitState.FINITE and limits.cpu_quota_domains:
            headrooms: list[float] = []
            for domain in limits.cpu_quota_domains:
                previous = self._cgroup_domains_before.get(domain.path)
                if previous is None or previous[0] != domain.quota_cores:
                    break
                usage = _rate(previous[1], domain.usage_seconds, elapsed)
                if usage is None:
                    break
                headrooms.append(max(0.0, domain.quota_cores - usage))
            else:
                quota_headroom = min(headrooms)
        throttled_rate = _rate(self._throttled_before, limits.throttled_seconds, elapsed)
        self._cpu_before = cpu_total.value
        self._cores_before = per_cpu.value
        self._process_before = process_cpu
        self._cgroup_cpu_before = limits.cpu_usage_seconds
        self._cgroup_domains_before = {
            domain.path: (domain.quota_cores, domain.usage_seconds)
            for domain in limits.cpu_quota_domains
        }
        self._throttled_before = limits.throttled_seconds
        self._previous_time = clock.monotonic

        cpu = CpuMetrics(
            logical_cores=_from_reading(
                logical, "cores", "psutil.cpu_count", Scope.MACHINE, clock, freshness
            ),
            physical_cores=_from_reading(
                physical, "cores", "psutil.cpu_count", Scope.MACHINE, clock, freshness
            ),
            utilization_percent=metric(
                aggregate_percent,
                "percent",
                "psutil.cpu_times",
                Scope.MACHINE,
                clock,
                freshness,
                status=cpu_total.status if cpu_total.value is None else None,
                detail=cpu_total.detail
                or (
                    None
                    if aggregate_percent is not None
                    else "two valid counter samples are required"
                ),
            ),
            per_core_utilization_percent=metric(
                core_percent,
                "percent",
                "psutil.cpu_times",
                Scope.MACHINE,
                clock,
                freshness,
                status=per_cpu.status if per_cpu.value is None else None,
                detail=per_cpu.detail
                or (
                    None if core_percent is not None else "two valid per-core samples are required"
                ),
            ),
            process_utilization_percent=metric(
                process_percent,
                "percent of one core",
                "psutil.Process.cpu_times",
                Scope.PROCESS,
                clock,
                freshness,
                status=process_times.status if process_times.value is None else None,
                detail=process_times.detail
                or (
                    None
                    if process_percent is not None
                    else "two valid process samples are required"
                ),
            ),
            affinity_cpus=_from_reading(
                affinity,
                "cpu indices",
                "psutil.Process.cpu_affinity",
                Scope.PROCESS,
                clock,
                freshness,
            ),
            quota_cores=_limit_metric(limits.cpu, "CPU equivalents", clock, freshness),
            quota_state=limits.cpu.state,
            cgroup_utilization_cores=metric(
                cgroup_cpu,
                "CPU equivalents",
                "cgroup",
                Scope.PROCESS,
                clock,
                freshness,
                status=MetricStatus.UNSUPPORTED
                if limits.cpu.state is LimitState.NOT_APPLICABLE
                else None,
                detail=limits.cpu_stats_detail
                or (
                    None if cgroup_cpu is not None else "two valid cgroup CPU samples are required"
                ),
            ),
            cgroup_headroom_cores=metric(
                quota_headroom,
                "CPU equivalents",
                "cgroup ancestor quotas and usage",
                Scope.PROCESS,
                clock,
                freshness,
                status=MetricStatus.UNSUPPORTED
                if limits.cpu.state is LimitState.NOT_APPLICABLE
                else MetricStatus.ESTIMATED
                if quota_headroom is not None
                else None,
                detail=(
                    "Minimum remaining quota across all finite ancestors, including siblings."
                    if quota_headroom is not None
                    else "Two valid usage samples are required for every finite quota ancestor."
                ),
            ),
            throttled_seconds=metric(
                limits.throttled_seconds,
                "seconds",
                "cgroup",
                Scope.PROCESS,
                clock,
                freshness,
                status=MetricStatus.UNSUPPORTED
                if limits.cpu.state is LimitState.NOT_APPLICABLE
                else None,
                detail=limits.cpu_stats_detail
                or (None if limits.throttled_seconds is not None else "throttling is unavailable"),
            ),
            throttled_seconds_per_second=metric(
                throttled_rate,
                "throttled seconds/second",
                "cgroup",
                Scope.PROCESS,
                clock,
                freshness,
                status=(
                    MetricStatus.ESTIMATED
                    if throttled_rate is not None
                    else MetricStatus.UNSUPPORTED
                    if limits.cpu.state is LimitState.NOT_APPLICABLE
                    else MetricStatus.UNAVAILABLE
                ),
                detail=limits.cpu_stats_detail
                or (
                    "Cumulative cgroup throttling delta; it can exceed wall time."
                    if throttled_rate is not None
                    else "two valid throttling samples are required"
                ),
            ),
            frequency_mhz=self._frequency,
            load_average_1m=_from_reading(
                load, "load-average tasks", "os.getloadavg", Scope.MACHINE, clock, freshness
            ),
            runnable_tasks=_runnable_tasks(clock, freshness),
            process_threads=_from_reading(
                threads, "threads", "psutil.Process.num_threads", Scope.PROCESS, clock, freshness
            ),
        )

        virtual = _read(psutil.virtual_memory)
        swap = _read(psutil.swap_memory)
        process_memory = _read(self._process.memory_info)
        if do_io or self._uss is None:
            process_full = _read(self._process.memory_full_info)
            uss = (
                getattr(process_full.value, "uss", None) if process_full.value is not None else None
            )
            uss_status = (
                process_full.status
                if process_full.value is None
                else MetricStatus.AVAILABLE
                if uss is not None
                else MetricStatus.UNSUPPORTED
            )
            self._uss = metric(
                uss,
                "bytes",
                "psutil.Process.memory_full_info",
                Scope.PROCESS,
                clock,
                io_freshness,
                status=uss_status,
                detail=process_full.detail,
            )
        memory_limit = (
            int(limits.memory.value)
            if limits.memory.state is LimitState.FINITE and limits.memory.value is not None
            else None
        )
        memory_status = (
            MetricStatus.AVAILABLE
            if memory_limit is not None
            else MetricStatus.UNSUPPORTED
            if limits.memory.state is LimitState.NOT_APPLICABLE
            else MetricStatus.UNAVAILABLE
        )
        memory = MemoryMetrics(
            total_bytes=metric(
                virtual.value.total if virtual.value else None,
                "bytes",
                "psutil.virtual_memory",
                Scope.MACHINE,
                clock,
                freshness,
                status=virtual.status,
                detail=virtual.detail,
            ),
            available_bytes=metric(
                virtual.value.available if virtual.value else None,
                "bytes",
                "psutil.virtual_memory",
                Scope.MACHINE,
                clock,
                freshness,
                status=virtual.status,
                detail=virtual.detail,
            ),
            used_bytes=metric(
                virtual.value.used if virtual.value else None,
                "bytes",
                "psutil.virtual_memory",
                Scope.MACHINE,
                clock,
                freshness,
                status=virtual.status,
                detail=virtual.detail,
            ),
            process_rss_bytes=metric(
                process_memory.value.rss if process_memory.value else None,
                "bytes",
                "psutil.Process.memory_info",
                Scope.PROCESS,
                clock,
                freshness,
                status=process_memory.status,
                detail=process_memory.detail,
            ),
            process_vms_bytes=metric(
                process_memory.value.vms if process_memory.value else None,
                "bytes",
                "psutil.Process.memory_info",
                Scope.PROCESS,
                clock,
                freshness,
                status=process_memory.status,
                detail=process_memory.detail
                or (
                    "On Windows psutil reports PagefileUsage, not total virtual address space."
                    if sys.platform == "win32"
                    else "Virtual address space, not resident physical memory."
                ),
            ),
            process_uss_bytes=self._uss,
            swap_total_bytes=metric(
                swap.value.total if swap.value else None,
                "bytes",
                "psutil.swap_memory",
                Scope.MACHINE,
                clock,
                freshness,
                status=swap.status,
                detail=swap.detail,
            ),
            swap_used_bytes=metric(
                swap.value.used if swap.value else None,
                "bytes",
                "psutil.swap_memory",
                Scope.MACHINE,
                clock,
                freshness,
                status=swap.status,
                detail=swap.detail,
            ),
            cgroup_limit_bytes=metric(
                memory_limit,
                "bytes",
                "cgroup",
                Scope.PROCESS,
                clock,
                freshness,
                status=memory_status,
                detail=limits.memory.detail or f"cgroup limit is {limits.memory.state.value}",
            ),
            cgroup_limit_state=limits.memory.state,
            cgroup_usage_bytes=metric(
                limits.memory_usage, "bytes", "cgroup", Scope.PROCESS, clock, freshness
            ),
            cgroup_available_bytes=metric(
                limits.memory_available, "bytes", "cgroup", Scope.PROCESS, clock, freshness
            ),
        )

        if do_io:
            self._disk, self._network, self._connections, io_warnings = self._sample_io(
                clock, optional=sample_io
            )
            self._io_warnings = tuple(io_warnings)
        pressure = PressureMetrics(
            cpu_percent=_pressure("cpu", clock, freshness),
            memory_percent=_pressure("memory", clock, freshness),
            io_percent=_pressure("io", clock, freshness),
        )
        assert self._disk is not None
        assert self._connections is not None
        return Telemetry(
            clock=clock,
            cpu=cpu,
            memory=memory,
            disk=self._disk,
            network=self._network,
            network_connections=self._connections,
            pressure=pressure,
            container_id=limits.container_id,
            duration_seconds=time.perf_counter() - started,
            cpu_seconds=time.thread_time() - cpu_started,
            warnings=self._io_warnings,
        )

    def _sample_io(
        self, clock: SampleClock, *, optional: bool
    ) -> tuple[DiskMetrics, tuple[NetworkInterfaceMetrics, ...], Metric[int], list[str]]:
        freshness = self._config.disk_network_interval_seconds * 2
        elapsed = clock.monotonic - self._io_time if self._io_time is not None else 0
        disk_reading = _read(psutil.disk_io_counters)
        disk_current = None
        disk_times = None
        if disk_reading.value is not None:
            data = disk_reading.value
            disk_current = (data.read_bytes, data.write_bytes, data.read_count, data.write_count)
            read_time = getattr(data, "read_time", None)
            write_time = getattr(data, "write_time", None)
            if isinstance(read_time, (int, float)) and isinstance(write_time, (int, float)):
                disk_times = (read_time, write_time)
        disk_rates = [
            _rate(self._disk_before[index] if self._disk_before else None, value, elapsed)
            for index, value in enumerate(disk_current or (None, None, None, None))
        ]
        latency_value = None
        latency_status = MetricStatus.UNAVAILABLE
        latency_detail = "two valid disk samples are required"
        if disk_reading.value is None:
            latency_status = disk_reading.status
            latency_detail = disk_reading.detail or "disk counters are unavailable"
        elif disk_times is None:
            latency_status = MetricStatus.UNSUPPORTED
            latency_detail = "disk read and write timing counters are not exposed"
        elif min(disk_times) < 0:
            latency_detail = "disk timing counters are invalid"
            disk_times = None
        elif self._disk_times_before is not None:
            read_time_rate = _rate(self._disk_times_before[0], disk_times[0], elapsed)
            write_time_rate = _rate(self._disk_times_before[1], disk_times[1], elapsed)
            read_rate, write_rate = disk_rates[2], disk_rates[3]
            if (
                read_time_rate is None
                or write_time_rate is None
                or read_rate is None
                or write_rate is None
            ):
                latency_detail = "disk counters reset or the sample interval is invalid"
            elif read_rate + write_rate <= 0:
                latency_detail = "no completed disk operations were observed"
            elif read_time_rate + write_time_rate <= 0:
                latency_detail = "disk timing did not advance or is below the available resolution"
            else:
                latency_value = (read_time_rate + write_time_rate) / (read_rate + write_rate)
                latency_status = MetricStatus.ESTIMATED
                latency_detail = (
                    "Host-wide mean from OS read/write counters; timing resolution and "
                    "device coverage vary, and this is not per-device or tail latency."
                )
        self._disk_before = disk_current
        self._disk_times_before = disk_times

        filesystems: list[FilesystemMetrics] = []
        warnings: list[str] = []
        partitions = _read(lambda: psutil.disk_partitions(all=False))
        if partitions.value is None:
            warnings.append(f"filesystem inventory unavailable: {partitions.detail}")
        else:
            for partition in partitions.value[:128]:
                remote = partition.fstype.lower() in {"nfs", "nfs4", "cifs", "smbfs"} or (
                    partition.mountpoint.startswith("\\\\")
                )
                usage = (
                    _Reading(
                        None, MetricStatus.UNAVAILABLE, "remote filesystem probing is disabled"
                    )
                    if remote
                    else _read(
                        lambda mountpoint=partition.mountpoint: psutil.disk_usage(mountpoint)
                    )
                )
                filesystems.append(
                    FilesystemMetrics(
                        device=partition.device,
                        mountpoint=partition.mountpoint,
                        total_bytes=metric(
                            usage.value.total if usage.value else None,
                            "bytes",
                            "psutil.disk_usage",
                            Scope.MACHINE,
                            clock,
                            freshness,
                            status=usage.status,
                            detail=usage.detail,
                        ),
                        free_bytes=metric(
                            usage.value.free if usage.value else None,
                            "bytes",
                            "psutil.disk_usage",
                            Scope.MACHINE,
                            clock,
                            freshness,
                            status=usage.status,
                            detail=usage.detail,
                        ),
                    )
                )
            if len(partitions.value) > 128:
                warnings.append("filesystem inventory was truncated at 128 entries")
        disk = DiskMetrics(
            filesystems=tuple(filesystems),
            read_bytes_per_second=metric(
                disk_rates[0],
                "bytes/second",
                "psutil.disk_io_counters",
                Scope.MACHINE,
                clock,
                freshness,
                status=disk_reading.status if disk_reading.value is None else None,
                detail=disk_reading.detail
                or (None if disk_rates[0] is not None else "two valid disk samples are required"),
            ),
            write_bytes_per_second=metric(
                disk_rates[1],
                "bytes/second",
                "psutil.disk_io_counters",
                Scope.MACHINE,
                clock,
                freshness,
                status=disk_reading.status if disk_reading.value is None else None,
                detail=disk_reading.detail
                or (None if disk_rates[1] is not None else "two valid disk samples are required"),
            ),
            read_operations_per_second=metric(
                disk_rates[2],
                "operations/second",
                "psutil.disk_io_counters",
                Scope.MACHINE,
                clock,
                freshness,
                status=disk_reading.status if disk_reading.value is None else None,
                detail=disk_reading.detail
                or (None if disk_rates[2] is not None else "two valid disk samples are required"),
            ),
            write_operations_per_second=metric(
                disk_rates[3],
                "operations/second",
                "psutil.disk_io_counters",
                Scope.MACHINE,
                clock,
                freshness,
                status=disk_reading.status if disk_reading.value is None else None,
                detail=disk_reading.detail
                or (None if disk_rates[3] is not None else "two valid disk samples are required"),
            ),
            queue_depth=metric(
                None,
                "operations",
                "psutil",
                Scope.MACHINE,
                clock,
                freshness,
                status=MetricStatus.UNSUPPORTED,
                detail="queue depth is not exposed portably",
            ),
            latency_ms=metric(
                latency_value,
                "milliseconds",
                "psutil.disk_io_counters",
                Scope.MACHINE,
                clock,
                freshness,
                status=latency_status,
                detail=latency_detail,
            ),
        )

        network_reading = _read(lambda: psutil.net_io_counters(pernic=True))
        stats = _read(psutil.net_if_stats)
        interfaces: list[NetworkInterfaceMetrics] = []
        next_network: dict[str, tuple[int, int, int, int]] = {}
        if network_reading.value is None:
            warnings.append(f"network counters unavailable: {network_reading.detail}")
        else:
            entries = sorted(network_reading.value.items())
            for name, counters in entries[:256]:
                current = (
                    counters.bytes_sent,
                    counters.bytes_recv,
                    counters.packets_sent,
                    counters.packets_recv,
                )
                previous = self._network_before.get(name)
                rates = [
                    _rate(previous[i] if previous else None, current[i], elapsed) for i in range(4)
                ]
                next_network[name] = current
                interface = stats.value.get(name) if stats.value is not None else None
                speed = (
                    float(interface.speed * 1_000_000)
                    if interface and interface.speed > 0
                    else None
                )
                interfaces.append(
                    NetworkInterfaceMetrics(
                        name=name,
                        link_speed_bps=metric(
                            speed,
                            "bits/second",
                            "psutil.net_if_stats",
                            Scope.MACHINE,
                            clock,
                            freshness,
                            status=stats.status if stats.value is None else None,
                            detail=stats.detail
                            or (None if speed is not None else "link speed is unavailable"),
                        ),
                        sent_bytes_per_second=metric(
                            rates[0],
                            "bytes/second",
                            "psutil.net_io_counters",
                            Scope.MACHINE,
                            clock,
                            freshness,
                        ),
                        received_bytes_per_second=metric(
                            rates[1],
                            "bytes/second",
                            "psutil.net_io_counters",
                            Scope.MACHINE,
                            clock,
                            freshness,
                        ),
                        sent_packets_per_second=metric(
                            rates[2],
                            "packets/second",
                            "psutil.net_io_counters",
                            Scope.MACHINE,
                            clock,
                            freshness,
                        ),
                        received_packets_per_second=metric(
                            rates[3],
                            "packets/second",
                            "psutil.net_io_counters",
                            Scope.MACHINE,
                            clock,
                            freshness,
                        ),
                        errors=metric(
                            counters.errin + counters.errout,
                            "errors",
                            "psutil.net_io_counters",
                            Scope.MACHINE,
                            clock,
                            freshness,
                        ),
                        drops=metric(
                            counters.dropin + counters.dropout,
                            "packets",
                            "psutil.net_io_counters",
                            Scope.MACHINE,
                            clock,
                            freshness,
                        ),
                    )
                )
            if len(entries) > 256:
                warnings.append("network interface inventory was truncated at 256 entries")
        self._network_before = next_network
        self._io_time = clock.monotonic
        if not self._config.sample_process_connections or not optional:
            connections = metric(
                None,
                "connections",
                "psutil.Process.net_connections",
                Scope.PROCESS,
                clock,
                freshness,
                status=MetricStatus.UNAVAILABLE,
                detail=(
                    "Connection sampling is disabled; set sample_process_connections to enable it."
                    if not self._config.sample_process_connections
                    else "Connection sampling is paused by the overhead policy."
                ),
            )
        else:
            count = _read(lambda: len(self._process.net_connections(kind="inet")))
            connections = metric(
                count.value,
                "connections",
                "psutil.Process.net_connections",
                Scope.PROCESS,
                clock,
                freshness,
                status=MetricStatus.ESTIMATED if count.value is not None else count.status,
                detail=count.detail
                or "Visible IPv4 and IPv6 sockets for this process; visibility may be incomplete.",
            )
        return disk, tuple(interfaces), connections, warnings
