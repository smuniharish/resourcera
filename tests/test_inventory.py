from collections.abc import Sequence

import anyio
import pytest
from refresh_engine import RefreshStatus

from resourcera.config import ResourceConfig
from resourcera.exceptions import ProviderError
from resourcera.inventory import InventoryRefresher, InventoryReport, probe_inventory


@pytest.mark.asyncio
async def test_refresh_preserves_inventory_when_discovery_is_incomplete() -> None:
    reports = iter(
        [
            InventoryReport(items={"cpu": {"cores": 4}, "interfaces": ["one"]}, complete=True),
            InventoryReport(items={"cpu": {"cores": 8}}, complete=False, diagnostics=("failed",)),
            InventoryReport(items={"cpu": {"cores": 8}}, complete=True),
        ]
    )

    async def probe() -> InventoryReport:
        return next(reports)

    refresher = InventoryRefresher(ResourceConfig(), probe=probe)
    try:
        first = await refresher.start()
        assert first.status is RefreshStatus.SUCCESS
        assert refresher.entries["cpu"] == {"cores": 4}
        assert refresher.entries["interfaces"] == ["one"]
        assert "configuration" in refresher.entries

        partial = await refresher.refresh()
        assert partial.status is RefreshStatus.SUCCESS
        assert refresher.complete is False
        assert refresher.entries["cpu"] == {"cores": 8}
        assert refresher.entries["interfaces"] == ["one"]
        assert refresher.diagnostics

        await refresher.refresh()
        assert refresher.entries["cpu"] == {"cores": 8}
        assert "interfaces" not in refresher.entries
    finally:
        await refresher.close()


@pytest.mark.asyncio
async def test_inventory_worker_runs_without_vendor_dependencies() -> None:
    report = await probe_inventory(ResourceConfig())
    assert "platform" in report.items
    assert "cpu" in report.items


@pytest.mark.asyncio
async def test_inventory_timeout_terminates_the_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    original = anyio.open_process
    children: list[anyio.abc.Process] = []

    async def record(
        command: Sequence[str], *, stdin: int, stdout: int, stderr: int
    ) -> anyio.abc.Process:
        process = await original(command, stdin=stdin, stdout=stdout, stderr=stderr)
        children.append(process)
        return process

    monkeypatch.setattr(anyio, "open_process", record)
    with pytest.raises(ProviderError, match="timed out"):
        await probe_inventory(ResourceConfig(provider_timeout_seconds=0.001))
    assert len(children) == 1
    assert children[0].returncode is not None


@pytest.mark.asyncio
async def test_shed_inventory_is_rebuilt_by_a_full_refresh() -> None:
    report = InventoryReport(
        items={"cpu": {"cores": 4}, "interfaces": ["one"], "filesystems": ["disk"]},
        complete=True,
    )

    async def probe() -> InventoryReport:
        return report

    refresher = InventoryRefresher(ResourceConfig(), probe=probe)
    try:
        await refresher.start()
        await refresher.shed_optional()
        assert "interfaces" not in refresher.entries
        await refresher.refresh()
        assert refresher.entries["interfaces"] == ["one"]
        assert refresher.entries["filesystems"] == ["disk"]
    finally:
        await refresher.close()
