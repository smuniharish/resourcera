"""Conservative multidimensional capacity estimates."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass

from resourcera.config import ResourceConfig
from resourcera.models import (
    AcceleratorDevice,
    CapacityResult,
    DeviceStatus,
    LimitState,
    Metric,
    MetricStatus,
    ResourceAvailability,
    ResourceSnapshot,
    WorkloadRequirements,
)

_USABLE = {MetricStatus.AVAILABLE, MetricStatus.ESTIMATED}


@dataclass(frozen=True, slots=True)
class ResourceClaims:
    """Logical reservations already held by cooperating workloads."""

    cpu: float = 0
    memory_bytes: int = 0
    disk_read_bytes_per_second: float = 0
    disk_write_bytes_per_second: float = 0
    network_bytes_per_second: float = 0
    accelerator_devices: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        values = (
            self.cpu,
            self.memory_bytes,
            self.disk_read_bytes_per_second,
            self.disk_write_bytes_per_second,
            self.network_bytes_per_second,
        )
        if any(not math.isfinite(value) or value < 0 for value in values):
            raise ValueError("reservation claims must be finite and nonnegative")


def _failure[T](
    reading: Metric[T], now: float, description: str, *, unit: str | None = None
) -> ResourceAvailability:
    status = reading.status_at(now)
    if status in _USABLE:
        status = MetricStatus.ERROR
    return ResourceAvailability(
        value=None,
        unit=unit or reading.unit,
        status=status,
        reason=f"{description}: {reading.detail or status.value}",
        sources=(reading.source,),
    )


def _available(
    value: float, unit: str, reason: str, sources: tuple[str, ...], confidence: float = 0.5
) -> ResourceAvailability:
    return ResourceAvailability(
        value=max(0.0, value),
        unit=unit,
        status=MetricStatus.ESTIMATED,
        reason=reason,
        sources=sources,
        confidence=confidence,
    )


def _pressure_factor(
    reading: Metric[float], name: str, now: float, assumptions: list[str]
) -> float:
    if reading.status_at(now) in _USABLE and reading.value is not None:
        if 0 <= reading.value <= 100:
            assumptions.append(f"{name} pressure reduces headroom by {reading.value:g}%.")
            return 1 - reading.value / 100
        assumptions.append(f"{name} pressure is invalid and could not be applied.")
    else:
        assumptions.append(f"{name} pressure is unavailable and was not applied.")
    return 1.0


def _cpu(
    snapshot: ResourceSnapshot,
    claims: ResourceClaims,
    margin: float,
    now: float,
    assumptions: list[str],
) -> ResourceAvailability:
    cpu = snapshot.cpu
    logical = cpu.logical_cores
    if logical.status_at(now) not in _USABLE or logical.value is None or logical.value <= 0:
        return _failure(
            logical, now, "logical CPU count is unavailable or invalid", unit="CPU equivalents"
        )
    affinity = cpu.affinity_cpus
    if affinity.status_at(now) not in _USABLE or affinity.value is None:
        return _failure(
            affinity, now, "process CPU affinity is unavailable", unit="CPU equivalents"
        )
    indices = set(affinity.value)
    if (
        not indices
        or len(indices) != len(affinity.value)
        or any(index < 0 or index >= logical.value for index in indices)
    ):
        return ResourceAvailability(
            value=None,
            unit="CPU equivalents",
            status=MetricStatus.ERROR,
            reason="process CPU affinity contains invalid or duplicate CPU indices",
            sources=(affinity.source,),
        )
    per_core = cpu.per_core_utilization_percent
    aggregate = cpu.utilization_percent
    if (
        per_core.status_at(now) in _USABLE
        and per_core.value is not None
        and len(per_core.value) >= logical.value
    ):
        utilization = [per_core.value[index] for index in indices]
        if any(value < 0 or value > 100 for value in utilization):
            return _failure(
                per_core, now, "per-core CPU utilization is invalid", unit="CPU equivalents"
            )
        idle = sum(1 - value / 100 for value in utilization)
        sources = (logical.source, affinity.source, per_core.source)
    elif (
        len(indices) == logical.value
        and aggregate.status_at(now) in _USABLE
        and aggregate.value is not None
        and 0 <= aggregate.value <= 100
    ):
        idle = logical.value * (1 - aggregate.value / 100)
        sources = (logical.source, affinity.source, aggregate.source)
    else:
        return _failure(
            per_core,
            now,
            "fresh utilization for the allowed CPUs is unavailable",
            unit="CPU equivalents",
        )
    if cpu.quota_state is LimitState.UNKNOWN:
        return _failure(
            cpu.quota_cores, now, "effective cgroup CPU limit is unknown", unit="CPU equivalents"
        )
    if cpu.quota_state is LimitState.FINITE:
        quota = cpu.quota_cores
        headroom = cpu.cgroup_headroom_cores
        if quota.status_at(now) not in _USABLE or quota.value is None or quota.value <= 0:
            return _failure(
                quota, now, "finite cgroup CPU quota is unavailable", unit="CPU equivalents"
            )
        if headroom.status_at(now) not in _USABLE or headroom.value is None or headroom.value < 0:
            return _failure(
                headroom,
                now,
                "cgroup ancestor CPU headroom is unavailable",
                unit="CPU equivalents",
            )
        idle = min(idle, headroom.value)
        sources += (quota.source, headroom.source)
        throttle = cpu.throttled_seconds_per_second
        if throttle.status_at(now) in _USABLE and throttle.value is not None:
            idle *= 1 - min(1.0, max(0.0, throttle.value))
            assumptions.append("Recent cgroup throttling further reduces estimated CPU headroom.")
        else:
            assumptions.append("Recent cgroup throttling was unavailable and was not applied.")
    pressure = _pressure_factor(snapshot.pressure.cpu_percent, "CPU", now, assumptions)
    value = idle * (1 - margin) * pressure - claims.cpu
    return _available(
        value,
        "CPU equivalents",
        "Estimated idle CPU within affinity and any known cgroup quota, less claims and margin.",
        sources,
    )


def _memory(
    snapshot: ResourceSnapshot,
    claims: ResourceClaims,
    margin: float,
    now: float,
    assumptions: list[str],
) -> ResourceAvailability:
    memory = snapshot.memory
    available = memory.available_bytes
    if available.status_at(now) not in _USABLE or available.value is None or available.value < 0:
        return _failure(available, now, "host available memory is unavailable")
    amount = float(available.value)
    sources = (available.source,)
    if memory.cgroup_limit_state is LimitState.UNKNOWN:
        return _failure(memory.cgroup_limit_bytes, now, "effective cgroup memory limit is unknown")
    if memory.cgroup_limit_state is LimitState.FINITE:
        limit = memory.cgroup_limit_bytes
        if limit.status_at(now) not in _USABLE or limit.value is None or limit.value < 0:
            return _failure(limit, now, "finite cgroup memory limit is unavailable", unit="bytes")
        cgroup = memory.cgroup_available_bytes
        if cgroup.status_at(now) not in _USABLE or cgroup.value is None or cgroup.value < 0:
            return _failure(cgroup, now, "cgroup memory headroom is unavailable")
        amount = min(amount, float(cgroup.value))
        sources += (cgroup.source,)
    pressure = _pressure_factor(snapshot.pressure.memory_percent, "Memory", now, assumptions)
    value = amount * (1 - margin) * pressure - claims.memory_bytes
    return _available(
        value,
        "bytes",
        "Host and cgroup headroom are intersected; process RSS is not subtracted again.",
        sources,
        confidence=0.7,
    )


def _throughput(
    limit: float | None,
    observed: Metric[float],
    reserved: float,
    margin: float,
    now: float,
    label: str,
) -> ResourceAvailability:
    if limit is None:
        return ResourceAvailability(
            value=None,
            unit="bytes/second",
            status=MetricStatus.UNAVAILABLE,
            reason=f"No usable {label} throughput ceiling was configured.",
        )
    if observed.status_at(now) not in _USABLE or observed.value is None or observed.value < 0:
        return _failure(observed, now, f"{label} throughput is unavailable", unit="bytes/second")
    return _available(
        max(0.0, limit - observed.value) * (1 - margin) - reserved,
        "bytes/second",
        f"Configured {label} ceiling less observed throughput, margin, and logical claims.",
        (observed.source, "configuration"),
    )


def _network(
    snapshot: ResourceSnapshot,
    config: ResourceConfig,
    claims: ResourceClaims,
    margin: float,
    now: float,
) -> ResourceAvailability:
    if config.network_capacity_bytes_per_second is None:
        return ResourceAvailability(
            value=None,
            unit="bytes/second",
            status=MetricStatus.UNAVAILABLE,
            reason="No combined network throughput ceiling was configured.",
        )
    if not snapshot.network:
        return ResourceAvailability(
            value=None,
            unit="bytes/second",
            status=MetricStatus.UNAVAILABLE,
            reason="No interface throughput observations are available.",
        )
    observed = 0.0
    sources: set[str] = set()
    for interface in snapshot.network:
        for reading in (interface.sent_bytes_per_second, interface.received_bytes_per_second):
            if reading.status_at(now) not in _USABLE or reading.value is None or reading.value < 0:
                return _failure(
                    reading,
                    now,
                    f"network interface {interface.name} is unavailable",
                    unit="bytes/second",
                )
            observed += reading.value
            sources.add(reading.source)
    value = max(0.0, config.network_capacity_bytes_per_second - observed)
    return _available(
        value * (1 - margin) - claims.network_bytes_per_second,
        "bytes/second",
        "Configured combined ceiling less visible interface traffic, margin, and logical claims.",
        (*sorted(sources), "configuration"),
    )


def _accelerators(
    snapshot: ResourceSnapshot,
    requirements: WorkloadRequirements | None,
    claims: ResourceClaims,
    margin: float,
    now: float,
) -> tuple[ResourceAvailability, tuple[str, ...]]:
    requested_type = (
        requirements.accelerator_type.strip().casefold()
        if requirements is not None and requirements.accelerator_type is not None
        else None
    )
    memory = (
        int(requirements.accelerator_memory)
        if requirements is not None and requirements.accelerator_memory is not None
        else None
    )
    candidates: list[str] = []
    uncertain = False
    groups: dict[str, list[AcceleratorDevice]] = {}
    for device in snapshot.accelerators:
        if requested_type is not None and requested_type not in {
            device.kind.casefold(),
            device.vendor.casefold() if device.vendor else "",
        }:
            continue
        identity = f"uuid:{device.uuid.casefold()}" if device.uuid else f"id:{device.id}"
        groups.setdefault(identity, []).append(device)
    for group in groups.values():
        if any(
            device.id in claims.accelerator_devices
            or (device.uuid is not None and device.uuid.casefold() in claims.accelerator_devices)
            for device in group
        ):
            continue
        free: list[float] = []
        for device in group:
            if any(
                status in device.statuses
                for status in (
                    DeviceStatus.DEVICE_INACCESSIBLE,
                    DeviceStatus.DEVICE_UNSUPPORTED,
                    DeviceStatus.PROVIDER_ERROR,
                    DeviceStatus.PROVIDER_NOT_INSTALLED,
                )
            ):
                uncertain = True
                break
            total, used = device.memory_total_bytes, device.memory_used_bytes
            if (
                total.status_at(now) not in _USABLE
                or used.status_at(now) not in _USABLE
                or total.value is None
                or used.value is None
                or total.value < 0
                or used.value < 0
                or used.value > total.value
            ):
                uncertain = True
                break
            free.append((total.value - used.value) * (1 - margin))
        else:
            if memory is None or min(free) >= memory:
                candidates.append(min(device.id for device in group))
    required_count = requirements.accelerator_count or 1 if requirements is not None else 1
    if uncertain and len(candidates) < required_count:
        return (
            ResourceAvailability(
                value=None,
                unit="devices",
                status=MetricStatus.UNAVAILABLE,
                reason="Matching devices lack fresh, usable per-device memory telemetry.",
            ),
            (),
        )
    if not groups:
        os_provider = next(
            (
                provider
                for provider in snapshot.providers
                if provider.name == "operating-system accelerator discovery"
            ),
            None,
        )
        providers_uncertain = (
            any(
                provider.name not in {"psutil", "refresh-engine inventory", os_provider.name}
                and any(
                    status in provider.statuses
                    for status in (
                        DeviceStatus.PROVIDER_ERROR,
                        DeviceStatus.PROVIDER_NOT_INSTALLED,
                        DeviceStatus.PARTIAL_TELEMETRY,
                    )
                )
                and (
                    requested_type is None
                    or not provider.supported_vendors
                    or requested_type
                    in {vendor.casefold() for vendor in provider.supported_vendors}
                    or requested_type
                    in {kind.casefold() for kind in provider.supported_device_types}
                )
                for provider in snapshot.providers
            )
            if os_provider is not None
            else True
        )
        if (
            os_provider is None
            or DeviceStatus.PARTIAL_TELEMETRY in os_provider.statuses
            or DeviceStatus.PROVIDER_ERROR in os_provider.statuses
            or providers_uncertain
        ):
            return (
                ResourceAvailability(
                    value=None,
                    unit="devices",
                    status=MetricStatus.UNAVAILABLE,
                    reason="Device discovery is incomplete or unavailable.",
                ),
                (),
            )
    candidates.sort()
    return (
        _available(
            float(len(candidates)),
            "devices",
            (
                "Fresh per-device memory is required; logical claims reserve entire devices."
                + (" Additional devices have unavailable telemetry." if uncertain else "")
            ),
            tuple(sorted({device.source for device in snapshot.accelerators})),
            confidence=0.4,
        ),
        tuple(candidates),
    )


def estimate_capacity(
    snapshot: ResourceSnapshot,
    config: ResourceConfig,
    requirements: WorkloadRequirements | None = None,
    claims: ResourceClaims | None = None,
    *,
    now: float | None = None,
) -> CapacityResult:
    """Estimate safe concurrent work from fresh measurements and logical claims."""
    if now is None:
        now = time.monotonic()
    if claims is None:
        claims = ResourceClaims()
    margin = (
        requirements.safety_margin
        if requirements is not None and requirements.safety_margin is not None
        else config.safety_margin
    )
    assumptions = [
        "Idle CPU is an estimate, not guaranteed allocatable CPU.",
        "Logical reservations can overlap observed usage and are conservatively charged in full.",
        "Device visibility and free memory do not establish compute availability or isolation.",
        "No capacity estimate guarantees future resource availability.",
    ]
    devices, candidates = _accelerators(snapshot, requirements, claims, margin, now)
    available = {
        "cpu": _cpu(snapshot, claims, margin, now, assumptions),
        "memory": _memory(snapshot, claims, margin, now, assumptions),
        "disk_read": _throughput(
            config.disk_read_capacity_bytes_per_second,
            snapshot.disk.read_bytes_per_second,
            claims.disk_read_bytes_per_second,
            margin,
            now,
            "disk read",
        ),
        "disk_write": _throughput(
            config.disk_write_capacity_bytes_per_second,
            snapshot.disk.write_bytes_per_second,
            claims.disk_write_bytes_per_second,
            margin,
            now,
            "disk write",
        ),
        "network": _network(snapshot, config, claims, margin, now),
        "accelerators": devices,
    }
    failed_sampler = next(
        (
            provider
            for provider in snapshot.providers
            if provider.name == "psutil" and DeviceStatus.PROVIDER_ERROR in provider.statuses
        ),
        None,
    )
    if failed_sampler is not None:
        for key in ("cpu", "memory", "disk_read", "disk_write", "network"):
            available[key] = ResourceAvailability(
                value=None,
                unit=available[key].unit,
                status=MetricStatus.ERROR,
                reason=(
                    "The latest system sampling attempt failed: "
                    f"{failed_sampler.detail or 'unknown error'}."
                ),
                sources=("psutil",),
            )
        assumptions.append("A failed system sampler invalidates required cached system estimates.")
    dimensions: list[tuple[str, float]] = []
    if requirements is not None:
        for key, demand in (
            ("cpu", requirements.cpu),
            ("memory", requirements.memory),
            ("disk_read", requirements.disk_read_bytes_per_second),
            ("disk_write", requirements.disk_write_bytes_per_second),
            ("network", requirements.network_bytes_per_second),
        ):
            if demand is not None:
                dimensions.append((key, float(demand)))
        if (
            requirements.accelerator_type is not None
            or requirements.accelerator_memory is not None
            or requirements.accelerator_count is not None
        ):
            dimensions.append(("accelerators", float(requirements.accelerator_count or 1)))
    unknown = [key for key, _ in dimensions if available[key].value is None]
    max_concurrency: int | None = None
    limiting: str | None = None
    if unknown:
        limiting = unknown[0]
        assumptions.append("Required unknown or stale measurements make concurrency unknown.")
    elif dimensions:
        limits: list[tuple[str, int]] = []
        for key, demand in dimensions:
            value = available[key].value
            if value is None:
                raise RuntimeError("required capacity unexpectedly became unavailable")
            limits.append((key, math.floor(value / demand)))
        limiting, max_concurrency = min(limits, key=lambda item: item[1])
        if max_concurrency > config.max_concurrency:
            max_concurrency = config.max_concurrency
            limiting = "max_concurrency"
        assumptions.append("The configured application concurrency ceiling is also enforced.")
    age = max(0.0, now - snapshot.cpu.logical_cores.sampled_monotonic)
    return CapacityResult(
        available=available,
        max_concurrency=max_concurrency,
        limiting_resource=limiting,
        assumptions=tuple(assumptions),
        snapshot_id=snapshot.id,
        snapshot_age_seconds=age,
        safety_margin=margin,
        accelerator_devices=candidates,
    )
