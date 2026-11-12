"""Observe cached hybrid samples. Requires: pip install resourcera."""

import anyio

from resourcera import ResourceConfig, ResourceManager


async def main() -> None:
    config = ResourceConfig(cpu_memory_interval_seconds=0.5, disk_network_interval_seconds=2)
    async with ResourceManager(config) as resources:
        first = await resources.snapshot()
        await anyio.sleep(0.7)
        second = await resources.snapshot()
        print("First snapshot:", first.id)
        print("Latest snapshot:", second.id)
        print("CPU metric status:", second.cpu.utilization_percent.status.value)
        print("Disk rate status:", second.disk.read_bytes_per_second.status.value)
        print("Sampling policy:", resources.config.sampling)


if __name__ == "__main__":
    anyio.run(main)
