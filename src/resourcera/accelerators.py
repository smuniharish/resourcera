"""Operating-system accelerator discovery and provider report types."""

from __future__ import annotations

import ctypes
import importlib.util
import platform
import re
import sys
import time
from datetime import UTC, datetime
from itertools import islice
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

from pydantic import Field, model_validator

from resourcera.exceptions import ProviderError
from resourcera.metrics import SampleClock, metric
from resourcera.models import (
    AcceleratorDevice,
    DeviceStatus,
    FrozenModel,
    Metric,
    MetricStatus,
    ProviderInfo,
    Scope,
)

if TYPE_CHECKING:
    from resourcera.config import ResourceConfig

_VENDORS = {
    0x1002: "AMD",
    0x10DE: "NVIDIA",
    0x1AE0: "Google",
    0x8086: "Intel",
    0x106B: "Apple",
}
_METRICS = (
    "memory_total_bytes",
    "memory_used_bytes",
    "utilization_percent",
    "process_memory_bytes",
)


class DeviceReading(FrozenModel):
    """A provider's bounded raw device observation."""

    id: str
    vendor: str | None = None
    kind: str
    model: str | None = None
    uuid: str | None = None
    driver_version: str | None = None
    source: str
    statuses: tuple[DeviceStatus, ...]
    memory_total_bytes: int | None = Field(default=None, ge=0)
    memory_used_bytes: int | None = Field(default=None, ge=0)
    utilization_percent: float | None = Field(default=None, ge=0, le=100)
    process_memory_bytes: int | None = Field(default=None, ge=0)
    metric_status: dict[str, MetricStatus] = Field(default_factory=dict)
    metric_detail: dict[str, str] = Field(default_factory=dict)
    detail: str | None = None
    sampled_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    sampled_monotonic: float = Field(default_factory=time.monotonic, ge=0)

    @model_validator(mode="after")
    def validate_metrics(self) -> DeviceReading:
        """Reject contradictory provider values and declared metric statuses."""
        for name in _METRICS:
            status = self.metric_status.get(name)
            if status is not None:
                has_value = getattr(self, name) is not None
                valid_value = status in {
                    MetricStatus.AVAILABLE,
                    MetricStatus.ESTIMATED,
                    MetricStatus.STALE,
                }
                if has_value != valid_value:
                    raise ValueError(f"{name} value contradicts its metric status")
        if self.sampled_at.utcoffset() is None:
            raise ValueError("device observation time must be timezone-aware")
        return self


class ProviderReport(FrozenModel):
    """Result returned by an isolated accelerator provider callable."""

    provider: ProviderInfo
    devices: tuple[DeviceReading, ...] = ()
    complete: bool = True
    sampled_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    sampled_monotonic: float = Field(default_factory=time.monotonic, ge=0)
    cpu_seconds: float | None = Field(default=None, ge=0)


class PlatformAccelerators(FrozenModel):
    """Devices visible to an OS interface and optional-provider availability."""

    devices: tuple[DeviceReading, ...]
    complete: bool
    diagnostics: tuple[str, ...] = ()
    nvidia_installed: bool | None
    nvidia_interface_detected: bool = False
    sampled_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    sampled_monotonic: float = Field(default_factory=time.monotonic, ge=0)
    cpu_seconds: float | None = Field(default=None, ge=0)


def _vendor_from_path(path: str) -> str | None:
    match = re.search(r"(?:VEN_|VID_)([0-9A-Fa-f]{4})", path)
    return _VENDORS.get(int(match.group(1), 16)) if match else None


def _detected(
    device_id: str, source: str, kind: str, vendor: str | None, detail: str
) -> DeviceReading:
    return DeviceReading(
        id=device_id,
        source=source,
        kind=kind,
        vendor=vendor,
        statuses=(DeviceStatus.DEVICE_DETECTED,),
        metric_status=dict.fromkeys(_METRICS, MetricStatus.UNSUPPORTED),
        detail=detail,
    )


def _linux_devices(root: Path) -> tuple[list[DeviceReading], list[str]]:
    entries = list(islice(root.iterdir(), 1025))
    errors = ["PCI inventory was truncated at 1024 entries"] if len(entries) > 1024 else []
    devices: list[DeviceReading] = []
    for path in sorted(entries[:1024]):
        try:
            class_code = int((path / "class").read_text(encoding="ascii").strip(), 16)
            category = class_code >> 16
            if category not in {0x03, 0x12} and class_code >> 8 != 0x0880:
                continue
            vendor_id = int((path / "vendor").read_text(encoding="ascii").strip(), 16)
            device_code = int((path / "device").read_text(encoding="ascii").strip(), 16)
        except (OSError, ValueError) as exc:
            errors.append(f"PCI device {path.name}: {exc}")
            continue
        edge_tpu = (vendor_id, device_code) == (0x1AC1, 0x089A)
        if category not in {0x03, 0x12} and not edge_tpu:
            continue
        if len(devices) >= 128:
            errors.append("accelerator inventory was truncated at 128 devices")
            break
        try:
            driver = (path / "driver").readlink().name
        except FileNotFoundError:
            driver = None
        except OSError as exc:
            driver = None
            errors.append(f"PCI device {path.name} driver: {exc}")
        kind = (
            "tpu"
            if edge_tpu
            else "npu"
            if vendor_id == 0x8086 and driver in {"intel_vpu", "ivpu"}
            else "gpu"
            if category == 0x03
            else "accelerator"
        )
        reading = _detected(
            f"pci:{path.name.lower()}",
            "linux-sysfs",
            kind,
            "Google" if edge_tpu else _VENDORS.get(vendor_id),
            f"PCI vendor {vendor_id:04x}, device {device_code:04x}; "
            "compute capability and accessibility are not established",
        )
        if edge_tpu:
            reading = reading.model_copy(update={"model": "Coral Edge TPU"})
        if vendor_id == 0x1002 and driver == "amdgpu":
            reading = _linux_amdgpu(path, reading)
        devices.append(reading)
    return devices, errors


def _linux_amdgpu(path: Path, reading: DeviceReading) -> DeviceReading:
    values: dict[str, int | float | None] = {}
    statuses: dict[str, MetricStatus] = {
        "process_memory_bytes": MetricStatus.UNSUPPORTED,
    }
    details: dict[str, str] = {}
    for metric_name, filename in (
        ("memory_total_bytes", "mem_info_vram_total"),
        ("memory_used_bytes", "mem_info_vram_used"),
        ("utilization_percent", "gpu_busy_percent"),
    ):
        try:
            value = int((path / filename).read_text(encoding="ascii").strip())
            if value < 0 or (metric_name == "utilization_percent" and value > 100):
                raise ValueError(f"{filename} is outside its expected range")
            values[metric_name] = value
        except FileNotFoundError:
            values[metric_name] = None
            statuses[metric_name] = MetricStatus.UNSUPPORTED
            details[metric_name] = f"{filename} is not exposed by the driver"
        except PermissionError as exc:
            values[metric_name] = None
            statuses[metric_name] = MetricStatus.UNAVAILABLE
            details[metric_name] = str(exc)
        except (OSError, ValueError) as exc:
            values[metric_name] = None
            statuses[metric_name] = MetricStatus.ERROR
            details[metric_name] = str(exc)
    if not any(value is not None for value in values.values()):
        return reading.model_copy(update={"metric_status": statuses, "metric_detail": details})
    return DeviceReading.model_validate(
        {
            **reading.model_dump(),
            **values,
            "source": "linux-amdgpu-sysfs",
            "statuses": (
                DeviceStatus.DEVICE_DETECTED,
                DeviceStatus.PROVIDER_AVAILABLE,
                DeviceStatus.PARTIAL_TELEMETRY,
            ),
            "metric_status": statuses,
            "metric_detail": details,
        }
    )


class _Guid(ctypes.Structure):
    _fields_ = [
        ("data1", ctypes.c_uint32),
        ("data2", ctypes.c_uint16),
        ("data3", ctypes.c_uint16),
        ("data4", ctypes.c_ubyte * 8),
    ]


def _windows_devices() -> tuple[list[DeviceReading], list[str]]:
    library = ctypes.WinDLL("cfgmgr32", use_last_error=True)
    size_fn = library.CM_Get_Device_Interface_List_SizeW
    list_fn = library.CM_Get_Device_Interface_ListW
    size_fn.argtypes = [
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.POINTER(_Guid),
        ctypes.c_wchar_p,
        ctypes.c_uint32,
    ]
    size_fn.restype = ctypes.c_uint32
    list_fn.argtypes = [
        ctypes.POINTER(_Guid),
        ctypes.c_wchar_p,
        ctypes.POINTER(ctypes.c_wchar),
        ctypes.c_uint32,
        ctypes.c_uint32,
    ]
    list_fn.restype = ctypes.c_uint32
    guid = _Guid.from_buffer_copy(UUID("5b45201d-f2f2-4f3b-85bb-30ff1f953599").bytes_le)
    for _ in range(3):
        length = ctypes.c_uint32()
        result = size_fn(ctypes.byref(length), ctypes.byref(guid), None, 0)
        if result != 0:
            raise OSError(f"ConfigMgr interface-size query failed: {result:#x}")
        if length.value > 262144:
            raise OSError("ConfigMgr interface list exceeds its size limit")
        buffer = ctypes.create_unicode_buffer(max(length.value, 1))
        result = list_fn(ctypes.byref(guid), None, buffer, length.value, 0)
        if result == 0:
            paths = [item for item in "".join(buffer[: length.value]).split("\0") if item]
            errors = ["display inventory was truncated at 128 devices"] if len(paths) > 128 else []
            return [
                _detected(
                    f"windows:{path}",
                    "windows-cfgmgr32",
                    "gpu" if _vendor_from_path(path) else "display-controller",
                    _vendor_from_path(path),
                    "present display-adapter interface; compute capability is not established",
                )
                for path in paths[:128]
            ], errors
        if result != 0x1A:
            raise OSError(f"ConfigMgr interface query failed: {result:#x}")
    raise OSError("ConfigMgr interface list changed repeatedly during discovery")


def _macos_devices() -> tuple[list[DeviceReading], list[str]]:
    library = ctypes.CDLL("/System/Library/Frameworks/IOKit.framework/IOKit")
    library.IOServiceMatching.argtypes = [ctypes.c_char_p]
    library.IOServiceMatching.restype = ctypes.c_void_p
    library.IOServiceGetMatchingServices.argtypes = [
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_uint32),
    ]
    library.IOServiceGetMatchingServices.restype = ctypes.c_int
    library.IOIteratorNext.argtypes = [ctypes.c_uint32]
    library.IOIteratorNext.restype = ctypes.c_uint32
    library.IORegistryEntryGetName.argtypes = [ctypes.c_uint32, ctypes.POINTER(ctypes.c_char)]
    library.IORegistryEntryGetName.restype = ctypes.c_int
    library.IORegistryEntryGetRegistryEntryID.argtypes = [
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_uint64),
    ]
    library.IORegistryEntryGetRegistryEntryID.restype = ctypes.c_int
    library.IOObjectRelease.argtypes = [ctypes.c_uint32]
    library.IOObjectRelease.restype = ctypes.c_int
    matching = library.IOServiceMatching(b"IOAccelerator")
    if not matching:
        raise OSError("IOKit could not create an IOAccelerator match")
    iterator = ctypes.c_uint32()
    result = library.IOServiceGetMatchingServices(0, matching, ctypes.byref(iterator))
    if result != 0:
        raise OSError(f"IOKit accelerator enumeration failed: {result:#x}")
    devices: list[DeviceReading] = []
    errors: list[str] = []
    try:
        while entry := library.IOIteratorNext(iterator.value):
            try:
                if len(devices) >= 128:
                    errors.append("IOKit inventory was truncated at 128 devices")
                    break
                name = ctypes.create_string_buffer(128)
                registry_id = ctypes.c_uint64()
                name_result = library.IORegistryEntryGetName(entry, name)
                id_result = library.IORegistryEntryGetRegistryEntryID(
                    entry, ctypes.byref(registry_id)
                )
                if name_result != 0 or id_result != 0:
                    errors.append(f"IOKit could not identify accelerator service {entry}")
                    continue
                service = name.value.decode("utf-8", errors="replace")
                vendor = (
                    "Apple" if platform.machine() == "arm64" and service.startswith("AGX") else None
                )
                devices.append(
                    _detected(
                        f"iokit:{registry_id.value:x}",
                        "macos-iokit",
                        "accelerator",
                        vendor,
                        f"IOAccelerator service {service}; telemetry is not exposed here",
                    )
                )
            finally:
                library.IOObjectRelease(entry)
    finally:
        library.IOObjectRelease(iterator.value)
    return devices, errors


def discover_platform() -> PlatformAccelerators:
    """Enumerate existing OS-visible devices without importing vendor SDKs."""
    devices: list[DeviceReading] = []
    diagnostics: list[str] = []
    try:
        if sys.platform == "linux":
            devices, diagnostics = _linux_devices(Path("/sys/bus/pci/devices"))
        elif sys.platform == "win32":
            devices, diagnostics = _windows_devices()
        elif sys.platform == "darwin":
            devices, diagnostics = _macos_devices()
        else:
            diagnostics.append(f"accelerator enumeration is unsupported on {sys.platform}")
    except (OSError, ValueError) as exc:
        diagnostics.append(f"accelerator enumeration failed: {exc}")
    try:
        installed = importlib.util.find_spec("pynvml") is not None
    except (ImportError, ValueError) as exc:
        diagnostics.append(f"NVML interface lookup failed: {exc}")
        installed = None
    return PlatformAccelerators(
        devices=tuple(devices),
        complete=not diagnostics,
        diagnostics=tuple(diagnostics),
        nvidia_installed=installed,
        nvidia_interface_detected=(
            sys.platform == "linux"
            and (Path("/dev/nvidiactl").exists() or Path("/proc/driver/nvidia/gpus").exists())
        ),
    )


def merge_platform(
    previous: PlatformAccelerators | None, current: PlatformAccelerators
) -> PlatformAccelerators:
    """Keep earlier devices after incomplete enumeration without refreshing their timestamps."""
    if previous is None or current.complete:
        return current
    devices = {device.id: device for device in previous.devices}
    devices.update({device.id: device for device in current.devices})
    return current.model_copy(update={"devices": tuple(devices.values())})


def normalize_device(
    reading: DeviceReading, clock: SampleClock, freshness: float
) -> AcceleratorDevice:
    """Add freshness, units, and explicit status to one provider observation."""

    def value_metric[T](name: str, value: T | None, unit: str) -> Metric[T]:
        return metric(
            value,
            unit,
            reading.source,
            Scope.MACHINE,
            clock,
            freshness,
            status=reading.metric_status.get(name),
            detail=reading.metric_detail.get(name),
        )

    return AcceleratorDevice(
        id=reading.id,
        vendor=reading.vendor,
        kind=reading.kind,
        model=reading.model,
        uuid=reading.uuid,
        driver_version=reading.driver_version,
        statuses=reading.statuses,
        source=reading.source,
        memory_total_bytes=value_metric("memory_total_bytes", reading.memory_total_bytes, "bytes"),
        memory_used_bytes=value_metric("memory_used_bytes", reading.memory_used_bytes, "bytes"),
        utilization_percent=value_metric(
            "utilization_percent", reading.utilization_percent, "percent"
        ),
        process_memory_bytes=value_metric(
            "process_memory_bytes", reading.process_memory_bytes, "bytes"
        ),
        detail=reading.detail,
    )


async def run_provider(kind: str, argument: str, config: ResourceConfig) -> ProviderReport:
    """Run and validate a vendor or application provider in a bounded child."""
    from pydantic import ValidationError

    from resourcera._subprocess import run_worker

    output = await run_worker("resourcera._provider_worker", config, kind, argument)
    try:
        return ProviderReport.model_validate_json(output)
    except ValidationError as exc:
        raise ProviderError(f"{kind} provider returned an invalid report: {exc}") from exc


async def run_platform(config: ResourceConfig) -> PlatformAccelerators:
    """Refresh OS accelerator inventory in a separately terminable process."""
    from pydantic import ValidationError

    from resourcera._subprocess import run_worker

    output = await run_worker("resourcera._provider_worker", config, "platform", "-")
    try:
        return PlatformAccelerators.model_validate_json(output)
    except ValidationError as exc:
        raise ProviderError(f"platform provider returned an invalid report: {exc}") from exc
