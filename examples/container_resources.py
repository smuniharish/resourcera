"""Inspect cgroup limits when present. Requires: pip install resourcera."""

import anyio

from resourcera import ResourceManager


async def main() -> None:
    async with ResourceManager() as resources:
        snapshot = await resources.snapshot()
        print("Container identity:", snapshot.container_id or "unknown")
        print("CPU quota state:", snapshot.cpu.quota_state.value)
        print(
            "CPU quota:",
            snapshot.cpu.quota_cores.value,
            snapshot.cpu.quota_cores.unit,
            snapshot.cpu.quota_cores.status.value,
        )
        print("Memory limit state:", snapshot.memory.cgroup_limit_state.value)
        print(
            "Memory limit:",
            snapshot.memory.cgroup_limit_bytes.value,
            snapshot.memory.cgroup_limit_bytes.unit,
            snapshot.memory.cgroup_limit_bytes.status.value,
        )


if __name__ == "__main__":
    anyio.run(main)
