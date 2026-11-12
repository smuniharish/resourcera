"""Inspect disk rates and filesystems. Requires: pip install resourcera."""

import anyio

from resourcera import ResourceConfig, ResourceManager


async def main() -> None:
    config = ResourceConfig(cpu_memory_interval_seconds=0.25, disk_network_interval_seconds=0.25)
    async with ResourceManager(config) as resources:
        await anyio.sleep(0.4)
        disk = (await resources.snapshot()).disk
        for label, reading in (
            ("Read rate", disk.read_bytes_per_second),
            ("Write rate", disk.write_bytes_per_second),
            ("Queue depth", disk.queue_depth),
            ("Latency", disk.latency_ms),
        ):
            print(label, reading.value, reading.unit, reading.status.value)
        for filesystem in disk.filesystems[:5]:
            print(filesystem.mountpoint, filesystem.free_bytes.value, "bytes free")


if __name__ == "__main__":
    anyio.run(main)
