"""Discover Linux accelerator-class entries. Requires: pip install resourcera.

Run from the repository root so the isolated worker can import this module.
"""

import sys
from itertools import islice
from pathlib import Path

import anyio

from resourcera import (
    DeviceReading,
    DeviceStatus,
    MetricStatus,
    ProviderInfo,
    ProviderReport,
    ResourceConfig,
    ResourceManager,
)


def discover() -> ProviderReport:
    """Report actually visible Linux accelerator entries without inventing telemetry."""
    root = Path("/sys/class/accel")
    if sys.platform != "linux":
        return ProviderReport(
            provider=ProviderInfo(
                name="Linux accelerator class example",
                statuses=(DeviceStatus.DEVICE_UNSUPPORTED,),
                supported_operating_systems=("Linux",),
                limitations=("The Linux accelerator class is unavailable.",),
            )
        )
    entries: list[Path] = []
    try:
        entries = list(islice(root.iterdir(), 129))
    except FileNotFoundError:
        return ProviderReport(
            provider=ProviderInfo(
                name="Linux accelerator class example",
                statuses=(DeviceStatus.DEVICE_UNSUPPORTED,),
                supported_operating_systems=("Linux",),
                limitations=("The Linux accelerator class is unavailable.",),
            )
        )
    except OSError as error:
        return ProviderReport(
            provider=ProviderInfo(
                name="Linux accelerator class example",
                statuses=(DeviceStatus.PROVIDER_ERROR,),
                detail=str(error),
            ),
            complete=False,
        )
    devices = tuple(
        DeviceReading(
            id=f"sysfs-accel:{entry.name}",
            kind="accelerator",
            source="linux-sysfs-accel",
            statuses=(DeviceStatus.DEVICE_DETECTED,),
            metric_status={
                "memory_total_bytes": MetricStatus.UNSUPPORTED,
                "memory_used_bytes": MetricStatus.UNSUPPORTED,
                "utilization_percent": MetricStatus.UNSUPPORTED,
                "process_memory_bytes": MetricStatus.UNSUPPORTED,
            },
            detail="Accelerator class entry; detailed telemetry is not provided.",
        )
        for entry in entries[:128]
        if entry.name.startswith("accel") and entry.name[5:].isdigit()
    )
    statuses = (
        (DeviceStatus.PROVIDER_AVAILABLE,)
        if devices
        else (DeviceStatus.PROVIDER_AVAILABLE, DeviceStatus.DEVICE_NOT_DETECTED)
    )
    return ProviderReport(
        provider=ProviderInfo(
            name="Linux accelerator class example",
            statuses=statuses,
            supported_operating_systems=("Linux",),
            supported_device_types=("accelerator",),
            limitations=("Detailed telemetry and device accessibility are not established.",),
        ),
        devices=devices,
        complete=len(entries) <= 128,
    )


async def main() -> None:
    config = ResourceConfig(accelerator_providers=("examples.custom_provider:discover",))
    async with ResourceManager(config) as resources:
        snapshot = await resources.snapshot()
        for provider in snapshot.providers:
            if provider.name == "Linux accelerator class example":
                print("Provider:", provider.name, [state.value for state in provider.statuses])
                print("Limitations:", provider.limitations)
        for device in snapshot.accelerators:
            if device.source == "linux-sysfs-accel":
                print("Detected:", device.id)
        if not any(
            provider.name == "Linux accelerator class example" for provider in snapshot.providers
        ):
            print("The custom provider was unavailable or suspended; inspect provider statuses.")


if __name__ == "__main__":
    anyio.run(main)
