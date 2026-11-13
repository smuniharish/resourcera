"""Bound file processing by declared disk throughput. Requires: pip install resourcera."""

import tempfile
from pathlib import Path

import anyio

from resourcera import ResourceConfig, ResourceManager


async def process(path: Path) -> int:
    data = await anyio.Path(path).read_bytes()
    await anyio.Path(path.with_suffix(".out")).write_bytes(data.upper())
    return len(data)


async def main() -> None:
    # Disk throughput ceilings are not exposed portably, so declare them explicitly.
    config = ResourceConfig(
        disk_read_capacity_bytes_per_second=500_000_000,
        disk_write_capacity_bytes_per_second=200_000_000,
        disk_network_interval_seconds=1,
    )
    requirements = {
        "disk_read_bytes_per_second": 50_000_000,
        "disk_write_bytes_per_second": 20_000_000,
        "profile": "disk-bound",
    }
    with tempfile.TemporaryDirectory() as directory:
        paths = [Path(directory, f"input-{number}.txt") for number in range(6)]
        for path in paths:
            path.write_text("resourcera\n" * 100)

        async with (
            ResourceManager(config) as manager,
            manager.adaptive_concurrency(requirements, max_concurrency=40) as controller,
        ):
            # Throughput needs two samples; until then the controller stays at the minimum.
            for _ in range(20):
                if controller.state.confidence > 0:
                    break
                await anyio.sleep(0.5)
                await controller.adjust()
            print("Disk-limited concurrency:", controller.current_limit)
            print("Limiting resource:", controller.state.limiting_resource)
            total = 0

            async def guarded(path: Path) -> None:
                nonlocal total
                async with controller.slot():
                    total += await process(path)

            async with anyio.create_task_group() as tasks:
                for path in paths:
                    tasks.start_soon(guarded, path)
            print("Processed bytes:", total)


if __name__ == "__main__":
    anyio.run(main)
