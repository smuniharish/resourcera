"""Inspect locally visible machine resources. Requires: pip install resourcera."""

import anyio

from resourcera import ResourceManager


async def main() -> None:
    async with ResourceManager(scope="machine") as resources:
        snapshot = await resources.snapshot()
        print("Host:", snapshot.host_id)
        print("Logical CPUs:", snapshot.cpu.logical_cores.value)
        print("Total memory:", snapshot.memory.total_bytes.value, "bytes")
        print("Filesystem count:", len(snapshot.disk.filesystems))
        print("Network interface count:", len(snapshot.network))
        print("Accelerators visible:", len(snapshot.accelerators))


if __name__ == "__main__":
    anyio.run(main)
