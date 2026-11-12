import os
import sys
import threading
from contextlib import suppress
from pathlib import Path
from types import ModuleType, SimpleNamespace

import anyio
import pytest
from pydantic import ValidationError

from resourcera import (
    DeviceReading,
    DeviceStatus,
    ProviderInfo,
    ProviderReport,
    ResourceConfig,
    ResourceManager,
)
from resourcera._provider_worker import nvidia_report
from resourcera.accelerators import (
    PlatformAccelerators,
    _linux_amdgpu,
    _linux_devices,
    merge_platform,
    run_provider,
)
from resourcera.exceptions import ManagerStateError, ProviderError
from resourcera.inventory import InventoryReport
from resourcera.models import MetricStatus


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


def test_linux_pci_discovery_distinguishes_devices_from_vendor_names(tmp_path: Path) -> None:
    def device(address: str, class_code: str, vendor: str, device_code: str = "0x1234") -> None:
        path = tmp_path / address
        path.mkdir()
        (path / "class").write_text(class_code, encoding="ascii")
        (path / "vendor").write_text(vendor, encoding="ascii")
        (path / "device").write_text(device_code, encoding="ascii")

    device("pci-01", "0x030000", "0x10de")
    device("pci-02", "0x120000", "0x1ae0")
    device("pci-03", "0x020000", "0x8086")
    device("pci-05", "0x088000", "0x1ac1", "0x089a")
    broken = tmp_path / "pci-04"
    broken.mkdir()
    (broken / "class").write_text("0x030000", encoding="ascii")

    found, errors = _linux_devices(tmp_path)
    assert [(item.vendor, item.kind) for item in found] == [
        ("NVIDIA", "gpu"),
        ("Google", "accelerator"),
        ("Google", "tpu"),
    ]
    assert all(item.memory_total_bytes is None for item in found)
    assert errors and "pci-04" in errors[0]


def test_amd_sysfs_reports_available_and_invalid_metrics_separately(tmp_path: Path) -> None:
    (tmp_path / "mem_info_vram_total").write_text("8192", encoding="ascii")
    (tmp_path / "mem_info_vram_used").write_text("1024", encoding="ascii")
    (tmp_path / "gpu_busy_percent").write_text("101", encoding="ascii")
    reading = DeviceReading(
        id="pci:amd",
        vendor="AMD",
        kind="gpu",
        source="linux-sysfs",
        statuses=(DeviceStatus.DEVICE_DETECTED,),
    )
    result = _linux_amdgpu(tmp_path, reading)
    assert result.memory_total_bytes == 8192
    assert result.memory_used_bytes == 1024
    assert result.utilization_percent is None
    assert result.metric_status["utilization_percent"] is MetricStatus.ERROR
    assert result.metric_status["process_memory_bytes"] is MetricStatus.UNSUPPORTED


def test_incomplete_platform_refresh_preserves_device_age() -> None:
    old = DeviceReading(
        id="old",
        kind="gpu",
        source="test",
        statuses=(DeviceStatus.DEVICE_DETECTED,),
        sampled_monotonic=10,
    )
    new = old.model_copy(update={"id": "new", "sampled_monotonic": 20})
    previous = PlatformAccelerators(devices=(old,), complete=True, nvidia_installed=False)
    partial = PlatformAccelerators(devices=(new,), complete=False, nvidia_installed=None)
    merged = merge_platform(previous, partial)
    assert [(item.id, item.sampled_monotonic) for item in merged.devices] == [
        ("old", 10),
        ("new", 20),
    ]
    assert merged.complete is False


def test_nvml_partial_metrics_and_inaccessible_device_are_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nvml = ModuleType("pynvml")

    class NVMLError(Exception):
        pass

    class NotSupportedError(NVMLError):
        pass

    class NoPermissionError(NVMLError):
        pass

    def handle(index: int) -> int:
        if index:
            raise NoPermissionError("not visible")
        return index

    def unsupported(_: int) -> None:
        raise NotSupportedError("utilization unsupported")

    for name, value in {
        "NVMLError": NVMLError,
        "NVMLError_NotSupported": NotSupportedError,
        "NVMLError_NoPermission": NoPermissionError,
        "nvmlInit": lambda: None,
        "nvmlShutdown": lambda: None,
        "nvmlSystemGetDriverVersion": lambda: "driver",
        "nvmlDeviceGetCount": lambda: 2,
        "nvmlDeviceGetHandleByIndex": handle,
        "nvmlDeviceGetName": lambda _: "GPU",
        "nvmlDeviceGetUUID": lambda _: "GPU-123",
        "nvmlDeviceGetPciInfo": lambda _: SimpleNamespace(busId="00000000:01:00.0"),
        "nvmlDeviceGetMemoryInfo": lambda _: SimpleNamespace(total=8192, used=1024),
        "nvmlDeviceGetUtilizationRates": unsupported,
        "nvmlDeviceGetComputeRunningProcesses": lambda _: [
            SimpleNamespace(pid=1234, usedGpuMemory=300)
        ],
        "nvmlDeviceGetGraphicsRunningProcesses": lambda _: [
            SimpleNamespace(pid=1234, usedGpuMemory=200)
        ],
    }.items():
        setattr(nvml, name, value)
    monkeypatch.setitem(sys.modules, "pynvml", nvml)

    report = nvidia_report(1234)
    first, second = report.devices
    assert first.id == "pci:0000:01:00.0"
    assert first.memory_total_bytes == 8192
    assert first.metric_status["utilization_percent"] is MetricStatus.UNSUPPORTED
    assert first.process_memory_bytes == 300
    assert first.metric_status["process_memory_bytes"] is MetricStatus.ESTIMATED
    assert DeviceStatus.DEVICE_INACCESSIBLE in second.statuses
    assert report.complete is False


@pytest.mark.anyio
async def test_missing_optional_provider_does_not_hide_detected_device(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    device = DeviceReading(
        id="pci:0000:01:00.0",
        vendor="NVIDIA",
        kind="gpu",
        source="linux-sysfs",
        statuses=(DeviceStatus.DEVICE_DETECTED,),
    )
    platform = PlatformAccelerators(devices=(device,), complete=True, nvidia_installed=False)

    async def probe(config: ResourceConfig) -> InventoryReport:
        return InventoryReport(
            items={"accelerators": platform.model_dump(mode="json")}, complete=True
        )

    monkeypatch.setattr("resourcera.inventory.probe_inventory", probe)
    async with ResourceManager() as manager:
        snapshot = await manager.snapshot()
        detected = snapshot.accelerators[0]
        assert DeviceStatus.PROVIDER_NOT_INSTALLED in detected.statuses
        assert detected.memory_total_bytes.value is None
        assert detected.memory_total_bytes.status is MetricStatus.UNAVAILABLE
        provider = next(item for item in snapshot.providers if item.name == "NVIDIA NVML")
        assert "resourcera[nvidia]" in (provider.detail or "")


@pytest.mark.anyio
async def test_failed_provider_does_not_disable_another(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    platform = PlatformAccelerators(
        devices=(), complete=True, nvidia_installed=True, nvidia_interface_detected=True
    )

    async def probe(config: ResourceConfig) -> InventoryReport:
        return InventoryReport(
            items={"accelerators": platform.model_dump(mode="json")}, complete=True
        )

    async def provider(kind: str, argument: str, config: ResourceConfig) -> ProviderReport:
        if kind == "nvidia":
            raise ProviderError("driver timed out")
        return ProviderReport(
            provider=ProviderInfo(
                name="custom",
                statuses=(DeviceStatus.PROVIDER_AVAILABLE,),
                sampling_supported=True,
            ),
            devices=(
                DeviceReading(
                    id="custom:1",
                    vendor="Example",
                    kind="accelerator",
                    source="custom",
                    statuses=(DeviceStatus.TELEMETRY_AVAILABLE,),
                    memory_total_bytes=4096,
                    memory_used_bytes=1024,
                ),
            ),
        )

    monkeypatch.setattr("resourcera.inventory.probe_inventory", probe)
    monkeypatch.setattr("resourcera.manager.run_provider", provider)
    async with ResourceManager(ResourceConfig(accelerator_providers=("example:probe",))) as manager:
        snapshot = await manager.snapshot()
        assert snapshot.accelerators[0].memory_total_bytes.value == 4096
        nvidia = next(item for item in snapshot.providers if item.name == "NVIDIA NVML")
        assert DeviceStatus.PROVIDER_ERROR in nvidia.statuses
        assert "timed out" in (nvidia.detail or "")


@pytest.mark.anyio
async def test_custom_provider_is_executed_in_a_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = (
        "from resourcera import DeviceReading, DeviceStatus, ProviderInfo, ProviderReport\n"
        "async def probe():\n"
        "    return ProviderReport(provider=ProviderInfo(name='child', "
        "statuses=(DeviceStatus.PROVIDER_AVAILABLE,)), "
        "devices=(DeviceReading(id='child:1', kind='accelerator', source='child', "
        "statuses=(DeviceStatus.DEVICE_DETECTED,)),))\n"
    )
    (tmp_path / "example_provider.py").write_text(source, encoding="utf-8")
    pythonpath = os.pathsep.join((str(tmp_path), os.environ.get("PYTHONPATH", "")))
    monkeypatch.setenv("PYTHONPATH", pythonpath)
    report = await run_provider("custom", "example_provider:probe", ResourceConfig())
    assert report.provider.name == "child"
    assert report.devices[0].id == "child:1"


@pytest.mark.anyio
async def test_custom_provider_timeout_is_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "slow_provider.py").write_text(
        "import time\ndef probe():\n    time.sleep(20)\n", encoding="utf-8"
    )
    pythonpath = os.pathsep.join((str(tmp_path), os.environ.get("PYTHONPATH", "")))
    monkeypatch.setenv("PYTHONPATH", pythonpath)
    with anyio.fail_after(2), pytest.raises(ProviderError, match="timed out"):
        await run_provider(
            "custom",
            "slow_provider:probe",
            ResourceConfig(provider_timeout_seconds=0.25),
        )


@pytest.mark.anyio
async def test_shutdown_cancels_inflight_on_demand_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    platform = PlatformAccelerators(devices=(), complete=True, nvidia_installed=False)
    entered = threading.Event()
    cancelled = threading.Event()
    calls = 0

    async def probe(config: ResourceConfig) -> InventoryReport:
        return InventoryReport(
            items={"accelerators": platform.model_dump(mode="json")}, complete=True
        )

    async def platform_probe(config: ResourceConfig) -> PlatformAccelerators:
        return platform

    async def provider(kind: str, argument: str, config: ResourceConfig) -> ProviderReport:
        nonlocal calls
        calls += 1
        if calls > 1:
            entered.set()
            try:
                await anyio.sleep_forever()
            finally:
                cancelled.set()
        return ProviderReport(
            provider=ProviderInfo(name="test", statuses=(DeviceStatus.PROVIDER_AVAILABLE,))
        )

    monkeypatch.setattr("resourcera.inventory.probe_inventory", probe)
    monkeypatch.setattr("resourcera.manager.run_platform", platform_probe)
    monkeypatch.setattr("resourcera.manager.run_provider", provider)
    manager = ResourceManager(ResourceConfig(accelerator_providers=("example:probe",)))
    await manager.start()

    async def request() -> None:
        with suppress(ManagerStateError):
            await manager.refresh_accelerators()

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(request)
        with anyio.fail_after(2):
            assert await anyio.to_thread.run_sync(entered.wait, 1)
            await manager.close()
        assert cancelled.is_set()


@pytest.mark.anyio
async def test_import_does_not_eagerly_import_vendor_sdk() -> None:
    result = await anyio.run_process(
        [
            sys.executable,
            "-c",
            "import sys, resourcera; print('pynvml' in sys.modules)",
        ]
    )
    assert result.stdout.strip() == b"False"


def test_provider_contract_rejects_bad_names_and_contradictory_metrics() -> None:
    with pytest.raises(ValidationError):
        ResourceConfig(accelerator_providers=("bad provider",))
    with pytest.raises(ValidationError):
        DeviceReading(
            id="x",
            kind="gpu",
            source="test",
            statuses=(DeviceStatus.DEVICE_DETECTED,),
            metric_status={"memory_total_bytes": MetricStatus.AVAILABLE},
        )
