"""Run an accelerator provider in an isolated, terminable process."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import re
import sys
import time
from collections.abc import Awaitable, Callable
from types import ModuleType

from resourcera.accelerators import DeviceReading, ProviderReport, discover_platform
from resourcera.models import DeviceStatus, MetricStatus, ProviderInfo


def _text(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _pci_id(value: object) -> str | None:
    match = re.fullmatch(
        r"([0-9a-fA-F]{4,8}):([0-9a-fA-F]{2}):([0-9a-fA-F]{2}\.[0-7])", _text(value)
    )
    if match is None:
        return None
    return f"pci:{int(match.group(1), 16):04x}:{match.group(2).lower()}:{match.group(3).lower()}"


def _nvml_info(statuses: tuple[DeviceStatus, ...], detail: str | None = None) -> ProviderInfo:
    return ProviderInfo(
        name="NVIDIA NVML",
        statuses=statuses,
        supported_vendors=("NVIDIA",),
        supported_device_types=("gpu",),
        supported_operating_systems=("Linux", "Windows"),
        required_dependencies=("nvidia-ml-py",),
        supported_metrics=("memory", "utilization", "process memory", "driver version"),
        sampling_supported=True,
        limitations=(
            "Device and process visibility depend on the driver and container configuration.",
            "Utilization is not allocatable compute capacity.",
        ),
        detail=detail,
    )


def _nvml_device(
    nvml: ModuleType, handle: object, index: int, process_id: int, driver: str | None
) -> DeviceReading:
    statuses: dict[str, MetricStatus] = {}
    details: dict[str, str] = {}
    error_type = nvml.NVMLError

    def query[T](names: tuple[str, ...], operation: Callable[[], T]) -> T | None:
        try:
            return operation()
        except (error_type, OSError) as exc:
            if isinstance(exc, nvml.NVMLError_NotSupported):
                status = MetricStatus.UNSUPPORTED
            elif isinstance(exc, nvml.NVMLError_NoPermission):
                status = MetricStatus.UNAVAILABLE
            else:
                status = MetricStatus.ERROR
            for name in names:
                statuses[name] = status
                details[name] = str(exc)
            return None

    model = query(("model",), lambda: nvml.nvmlDeviceGetName(handle))
    uuid = query(("uuid",), lambda: nvml.nvmlDeviceGetUUID(handle))
    pci = query(("pci",), lambda: nvml.nvmlDeviceGetPciInfo(handle))
    memory = query(
        ("memory_total_bytes", "memory_used_bytes"),
        lambda: nvml.nvmlDeviceGetMemoryInfo(handle),
    )
    utilization = query(
        ("utilization_percent",), lambda: nvml.nvmlDeviceGetUtilizationRates(handle)
    )
    compute = query(
        ("process_memory_bytes",), lambda: nvml.nvmlDeviceGetComputeRunningProcesses(handle)
    )
    graphics = query(
        ("process_memory_bytes",), lambda: nvml.nvmlDeviceGetGraphicsRunningProcesses(handle)
    )
    process_memory: int | None = None
    if compute is not None and graphics is not None:
        matching = [
            item.usedGpuMemory
            for item in (*compute, *graphics)
            if item.pid == process_id
            and item.usedGpuMemory is not None
            and item.usedGpuMemory < (2**64 - 1)
        ]
        if matching:
            process_memory = max(matching)
            statuses["process_memory_bytes"] = MetricStatus.ESTIMATED
            details["process_memory_bytes"] = (
                "maximum matching compute/graphics record; PID namespace visibility may differ"
            )
        else:
            statuses["process_memory_bytes"] = MetricStatus.UNAVAILABLE
            details["process_memory_bytes"] = "no process with this visible PID was reported"
    pci_id = _pci_id(pci.busId) if pci is not None else None
    uuid_text = _text(uuid) if uuid is not None else None
    device_id = pci_id or (f"nvml:{uuid_text}" if uuid_text else f"nvml:index:{index}")
    partial = any(name in statuses for name in _METRIC_NAMES)
    return DeviceReading(
        id=device_id,
        vendor="NVIDIA",
        kind="gpu",
        model=_text(model) if model is not None else None,
        uuid=uuid_text,
        driver_version=driver,
        source="nvidia-nvml",
        statuses=(
            DeviceStatus.DEVICE_DETECTED,
            DeviceStatus.PROVIDER_AVAILABLE,
            DeviceStatus.PARTIAL_TELEMETRY if partial else DeviceStatus.TELEMETRY_AVAILABLE,
        ),
        memory_total_bytes=memory.total if memory is not None else None,
        memory_used_bytes=memory.used if memory is not None else None,
        utilization_percent=float(utilization.gpu) if utilization is not None else None,
        process_memory_bytes=process_memory,
        metric_status=statuses,
        metric_detail=details,
        detail="; ".join(
            f"{name}: {details[name]}" for name in ("model", "uuid", "pci") if name in details
        )
        or None,
    )


_METRIC_NAMES = {
    "memory_total_bytes",
    "memory_used_bytes",
    "utilization_percent",
    "process_memory_bytes",
}


def nvidia_report(process_id: int) -> ProviderReport:
    """Query the optional NVML binding with per-device failure isolation."""
    nvml = importlib.import_module("pynvml")
    error_type = nvml.NVMLError
    try:
        nvml.nvmlInit()
    except (error_type, OSError) as exc:
        return ProviderReport(
            provider=_nvml_info(
                (DeviceStatus.PROVIDER_ERROR,), f"NVML initialization failed: {exc}"
            ),
            complete=False,
        )
    devices: list[DeviceReading] = []
    errors: list[str] = []
    complete = True
    try:
        try:
            driver = _text(nvml.nvmlSystemGetDriverVersion())
        except (error_type, OSError) as exc:
            driver = None
            errors.append(f"driver version: {exc}")
        try:
            count = nvml.nvmlDeviceGetCount()
        except (error_type, OSError) as exc:
            count = 0
            complete = False
            errors.append(f"device enumeration failed: {exc}")
        if count > 128:
            complete = False
            errors.append("NVML inventory was truncated at 128 devices")
        for index in range(min(count, 128)):
            try:
                handle = nvml.nvmlDeviceGetHandleByIndex(index)
                devices.append(_nvml_device(nvml, handle, index, process_id, driver))
            except (error_type, OSError) as exc:
                complete = False
                errors.append(f"device {index} inaccessible: {exc}")
                devices.append(
                    DeviceReading(
                        id=f"nvml:index:{index}",
                        vendor="NVIDIA",
                        kind="gpu",
                        source="nvidia-nvml",
                        statuses=(DeviceStatus.DEVICE_DETECTED, DeviceStatus.DEVICE_INACCESSIBLE),
                        detail=str(exc),
                    )
                )
    finally:
        try:
            nvml.nvmlShutdown()
        except (error_type, OSError) as exc:
            complete = False
            errors.append(f"NVML shutdown failed: {exc}")
    provider_status = [DeviceStatus.PROVIDER_AVAILABLE]
    if not devices and complete:
        provider_status.append(DeviceStatus.DEVICE_NOT_DETECTED)
    if not complete or errors:
        provider_status.append(DeviceStatus.PARTIAL_TELEMETRY)
    return ProviderReport(
        provider=_nvml_info(tuple(provider_status), "; ".join(errors) or None),
        devices=tuple(devices),
        complete=complete,
    )


def _custom_report(path: str) -> ProviderReport:
    module, name = path.split(":", 1)
    provider = getattr(importlib.import_module(module), name)
    result = provider()
    if inspect.isawaitable(result):

        async def wait(awaitable: Awaitable[object]) -> object:
            return await awaitable

        result = asyncio.run(wait(result))
    return ProviderReport.model_validate(result)


def main() -> None:
    """Emit one validated provider report to the supervising process."""
    if len(sys.argv) != 3:
        raise ValueError("provider worker requires a kind and an argument")
    kind, argument = sys.argv[1:]
    if kind == "platform":
        report = discover_platform().model_copy(update={"cpu_seconds": time.process_time()})
        print(report.model_dump_json())
        return
    if kind == "nvidia":
        report = nvidia_report(int(argument))
    elif kind == "custom":
        report = _custom_report(argument)
    else:
        raise ValueError(f"unknown provider kind: {kind}")
    report = report.model_copy(update={"cpu_seconds": time.process_time()})
    print(report.model_dump_json())


if __name__ == "__main__":
    main()
