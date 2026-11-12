"""Print a cached snapshot. Requires: pip install resourcera."""

import anyio

from resourcera import ResourceManager


async def main() -> None:
    async with ResourceManager() as resources:
        snapshot = await resources.snapshot()
        print("Snapshot:", snapshot.id, snapshot.timestamp, snapshot.scope.value)
        print(
            "Available memory:",
            snapshot.memory.available_bytes.value,
            snapshot.memory.available_bytes.unit,
            snapshot.memory.available_bytes.status.value,
        )


if __name__ == "__main__":
    anyio.run(main)
