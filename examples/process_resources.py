"""Inspect current-process resources. Requires: pip install resourcera."""

import anyio

from resourcera import ResourceManager


async def main() -> None:
    async with ResourceManager(scope="process") as resources:
        snapshot = await resources.snapshot()
        print("Process:", snapshot.process_id)
        for label, reading in (
            ("RSS", snapshot.memory.process_rss_bytes),
            ("USS", snapshot.memory.process_uss_bytes),
            ("CPU", snapshot.cpu.process_utilization_percent),
            ("Threads", snapshot.cpu.process_threads),
        ):
            print(label, reading.value, reading.unit, reading.status.value)


if __name__ == "__main__":
    anyio.run(main)
