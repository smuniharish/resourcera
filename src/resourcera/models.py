"""Typed resource measurements, requirements, and decisions."""

from __future__ import annotations

import time
from datetime import datetime
from enum import StrEnum

from pydantic import (
    BaseModel,
    ByteSize,
    ConfigDict,
    Field,
    computed_field,
    field_validator,
    model_validator,
)


class Scope(StrEnum):
    """Visibility or coordination boundary."""

    PROCESS = "process"
    MACHINE = "machine"
    DISTRIBUTED = "distributed"


class MetricStatus(StrEnum):
    """Availability and quality of a measurement."""

    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    UNSUPPORTED = "unsupported"
    STALE = "stale"
    ESTIMATED = "estimated"
    ERROR = "error"


class DeviceStatus(StrEnum):
    """Device and provider capability states."""

    DEVICE_NOT_DETECTED = "device_not_detected"
    DEVICE_DETECTED = "device_detected"
    PROVIDER_NOT_INSTALLED = "provider_not_installed"
    PROVIDER_AVAILABLE = "provider_available"
    DEVICE_INACCESSIBLE = "device_inaccessible"
    TELEMETRY_AVAILABLE = "telemetry_available"
    PARTIAL_TELEMETRY = "partial_telemetry"
    PROVIDER_ERROR = "provider_error"
    DEVICE_UNSUPPORTED = "device_unsupported"


class LimitState(StrEnum):
    """Whether an OS limit is known, absent, or unknown."""

    FINITE = "finite"
    UNLIMITED = "unlimited"
    UNKNOWN = "unknown"
    NOT_APPLICABLE = "not_applicable"


class FrozenModel(BaseModel):
    """Base for immutable public records."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)


class Metric[T](FrozenModel):
    """A value with explicit units, origin, freshness, and quality."""

    value: T | None
    unit: str
    source: str
    scope: Scope
    sampled_at: datetime
    sampled_monotonic: float = Field(ge=0)
    fresh_for_seconds: float = Field(gt=0)
    measurement_status: MetricStatus
    detail: str | None = None
    confidence: float | None = Field(default=None, ge=0, le=1)

    @model_validator(mode="after")
    def validate_measurement(self) -> Metric[T]:
        """Reject contradictory value and status combinations."""
        has_value = self.measurement_status in {
            MetricStatus.AVAILABLE,
            MetricStatus.ESTIMATED,
            MetricStatus.STALE,
        }
        if has_value != (self.value is not None):
            raise ValueError("metric value must match its measurement status")
        if self.sampled_at.utcoffset() is None:
            raise ValueError("sampled_at must be timezone-aware")
        return self

    def status_at(self, monotonic_time: float) -> MetricStatus:
        """Return the status at a supplied monotonic time."""
        if (
            self.measurement_status in {MetricStatus.AVAILABLE, MetricStatus.ESTIMATED}
            and monotonic_time - self.sampled_monotonic > self.fresh_for_seconds
        ):
            return MetricStatus.STALE
        return self.measurement_status

    @computed_field
    @property
    def status(self) -> MetricStatus:
        """Current status, including expiration without a new sample."""
        return self.status_at(time.monotonic())

    @computed_field
    @property
    def age_seconds(self) -> float:
        """Age of this observation on the current monotonic clock."""
        return max(0.0, time.monotonic() - self.sampled_monotonic)


class CpuMetrics(FrozenModel):
    """CPU capacity, utilization, and process measurements."""

    logical_cores: Metric[int]
    physical_cores: Metric[int]
    utilization_percent: Metric[float]
    per_core_utilization_percent: Metric[tuple[float, ...]]
    process_utilization_percent: Metric[float]
    affinity_cpus: Metric[tuple[int, ...]]
    quota_cores: Metric[float]
    quota_state: LimitState
    cgroup_utilization_cores: Metric[float]
    cgroup_headroom_cores: Metric[float]
    throttled_seconds: Metric[float]
    throttled_seconds_per_second: Metric[float]
    frequency_mhz: Metric[float]
    load_average_1m: Metric[float]
    runnable_tasks: Metric[int]
    process_threads: Metric[int]


class MemoryMetrics(FrozenModel):
    """Host, process, and cgroup memory measurements in bytes."""

    total_bytes: Metric[int]
    available_bytes: Metric[int]
    used_bytes: Metric[int]
    process_rss_bytes: Metric[int]
    process_vms_bytes: Metric[int]
    process_uss_bytes: Metric[int]
    swap_total_bytes: Metric[int]
    swap_used_bytes: Metric[int]
    cgroup_limit_bytes: Metric[int]
    cgroup_limit_state: LimitState
    cgroup_usage_bytes: Metric[int]
    cgroup_available_bytes: Metric[int]


class FilesystemMetrics(FrozenModel):
    """Capacity of an accessible mounted filesystem."""

    device: str
    mountpoint: str
    total_bytes: Metric[int]
    free_bytes: Metric[int]


class DiskMetrics(FrozenModel):
    """Disk rates and filesystem capacity."""

    filesystems: tuple[FilesystemMetrics, ...]
    read_bytes_per_second: Metric[float]
    write_bytes_per_second: Metric[float]
    read_operations_per_second: Metric[float]
    write_operations_per_second: Metric[float]
    queue_depth: Metric[float]
    latency_ms: Metric[float]


class NetworkInterfaceMetrics(FrozenModel):
    """A network interface's observed rates and link speed."""

    name: str
    link_speed_bps: Metric[float]
    sent_bytes_per_second: Metric[float]
    received_bytes_per_second: Metric[float]
    sent_packets_per_second: Metric[float]
    received_packets_per_second: Metric[float]
    errors: Metric[int]
    drops: Metric[int]


class PressureMetrics(FrozenModel):
    """Pressure stall indicators where the OS exposes them."""

    cpu_percent: Metric[float]
    memory_percent: Metric[float]
    io_percent: Metric[float]


class AcceleratorDevice(FrozenModel):
    """An OS-visible accelerator and its independently available telemetry."""

    id: str
    vendor: str | None
    kind: str
    model: str | None = None
    uuid: str | None = None
    driver_version: str | None = None
    statuses: tuple[DeviceStatus, ...]
    source: str
    memory_total_bytes: Metric[int]
    memory_used_bytes: Metric[int]
    utilization_percent: Metric[float]
    process_memory_bytes: Metric[int]
    detail: str | None = None


class ProviderInfo(FrozenModel):
    """Provider health and supported capabilities."""

    name: str
    statuses: tuple[DeviceStatus, ...]
    supported_vendors: tuple[str, ...] = ()
    supported_device_types: tuple[str, ...] = ()
    supported_operating_systems: tuple[str, ...] = ()
    required_dependencies: tuple[str, ...] = ()
    supported_metrics: tuple[str, ...] = ()
    discovery_supported: bool = True
    sampling_supported: bool = False
    limitations: tuple[str, ...] = ()
    detail: str | None = None


class OverheadMetrics(FrozenModel):
    """Attributable monitoring work and explicitly limited state."""

    cpu_seconds: Metric[float]
    bootstrap_cpu_seconds: Metric[float]
    cpu_budget_seconds: float = Field(gt=0)
    managed_state_bytes: Metric[int]
    owned_tasks: Metric[int]
    owned_threads: Metric[int]
    sampling_duration_seconds: Metric[float]
    provider_cpu_seconds: Metric[float]
    provider_duration_seconds: Metric[float]
    accelerator_poll_cpu_seconds: Metric[float]
    disk_activity_bytes: Metric[int]
    network_activity_bytes: Metric[int]
    state: str
    degraded: bool


class ResourceSnapshot(FrozenModel):
    """A normalized, cached observation of visible resources."""

    id: str
    timestamp: datetime
    scope: Scope
    host_id: str | None
    process_id: int | None
    container_id: str | None
    cpu: CpuMetrics
    memory: MemoryMetrics
    disk: DiskMetrics
    network: tuple[NetworkInterfaceMetrics, ...]
    network_connections: Metric[int]
    accelerators: tuple[AcceleratorDevice, ...]
    pressure: PressureMetrics
    providers: tuple[ProviderInfo, ...]
    overhead: OverheadMetrics
    sampling_duration_seconds: float = Field(ge=0)


class WorkloadProfile(StrEnum):
    """Names for workload classes; demands must still be supplied explicitly."""

    CPU_BOUND = "cpu-bound"
    MEMORY_BOUND = "memory-bound"
    IO_BOUND = "io-bound"
    NETWORK_BOUND = "network-bound"
    GPU_BOUND = "gpu-bound"
    MIXED = "mixed"


class WorkloadRequirements(FrozenModel):
    """Per-workload resource requirements; accelerator memory is per device."""

    cpu: float | None = Field(default=None, gt=0)
    memory: ByteSize | None = None
    disk_read_bytes_per_second: float | None = Field(default=None, gt=0)
    disk_write_bytes_per_second: float | None = Field(default=None, gt=0)
    network_bytes_per_second: float | None = Field(default=None, gt=0)
    accelerator_type: str | None = None
    accelerator_memory: ByteSize | None = None
    accelerator_count: int | None = Field(default=None, gt=0)
    priority: int = 0
    safety_margin: float | None = Field(default=None, ge=0, lt=1)
    profile: WorkloadProfile | str | None = None

    @field_validator("accelerator_type", "profile")
    @classmethod
    def validate_label(cls, value: str | None) -> str | None:
        """Reject blank accelerator or custom-profile labels."""
        if value is not None and not value.strip():
            raise ValueError("labels must not be blank")
        return value

    @model_validator(mode="after")
    def require_demand(self) -> WorkloadRequirements:
        """Require a measurable resource demand."""
        if self.memory is not None and self.memory <= 0:
            raise ValueError("memory must be positive")
        if self.accelerator_memory is not None and self.accelerator_memory <= 0:
            raise ValueError("accelerator memory must be positive")
        if all(
            value is None
            for value in (
                self.cpu,
                self.memory,
                self.disk_read_bytes_per_second,
                self.disk_write_bytes_per_second,
                self.network_bytes_per_second,
                self.accelerator_type,
                self.accelerator_memory,
                self.accelerator_count,
            )
        ):
            raise ValueError("at least one resource requirement is required")
        return self


class ResourceAvailability(FrozenModel):
    """An available amount and the reason it can or cannot be estimated."""

    value: float | None = Field(ge=0)
    unit: str
    status: MetricStatus
    reason: str
    sources: tuple[str, ...] = ()
    confidence: float | None = Field(default=None, ge=0, le=1)

    @model_validator(mode="after")
    def validate_availability(self) -> ResourceAvailability:
        """Do not represent unknown capacity as a numeric amount."""
        has_value = self.status in {MetricStatus.AVAILABLE, MetricStatus.ESTIMATED}
        if has_value != (self.value is not None):
            raise ValueError("availability value must match its status")
        return self


class CapacityResult(FrozenModel):
    """An explainable capacity estimate, not a reservation or guarantee."""

    available: dict[str, ResourceAvailability]
    max_concurrency: int | None = Field(ge=0)
    limiting_resource: str | None
    assumptions: tuple[str, ...]
    snapshot_id: str
    snapshot_age_seconds: float = Field(ge=0)
    safety_margin: float = Field(ge=0, lt=1)
    accelerator_devices: tuple[str, ...] = ()


class AdmissionDecision(FrozenModel):
    """An admission outcome and its supporting capacity estimate."""

    accepted: bool
    requirements: WorkloadRequirements
    capacity: CapacityResult
    policy: str
    reason: str
    lease_id: str | None = None


class ResourceLease(FrozenModel):
    """A logical reservation owned by one resource manager."""

    id: str
    owner_id: str
    scope: Scope
    expires_at: datetime
    expires_monotonic: float = Field(ge=0)
    requirements: WorkloadRequirements
    accelerator_devices: tuple[str, ...] = ()
    accelerator_uuids: tuple[str, ...] = ()
    decision: AdmissionDecision

    @model_validator(mode="after")
    def validate_lease(self) -> ResourceLease:
        """Ensure the lease agrees with its accepted admission decision."""
        if self.expires_at.utcoffset() is None:
            raise ValueError("expires_at must be timezone-aware")
        if (
            not self.decision.accepted
            or self.decision.lease_id != self.id
            or self.decision.requirements != self.requirements
            or len(set(self.accelerator_devices)) != len(self.accelerator_devices)
            or len(set(self.accelerator_uuids)) != len(self.accelerator_uuids)
        ):
            raise ValueError("lease and admission decision are inconsistent")
        return self
