"""Estimate multiple requirements. Requires: pip install resourcera."""

import anyio

from resourcera import ResourceConfig, ResourceManager, WorkloadRequirements


async def main() -> None:
    config = ResourceConfig(cpu_memory_interval_seconds=0.25)
    requirements = WorkloadRequirements(cpu=0.25, memory="64MiB", profile="mixed")
    async with ResourceManager(config) as resources:
        await anyio.sleep(0.4)
        result = await resources.capacity(requirements)
        print("Estimated concurrency:", result.max_concurrency)
        print("Limiting resource:", result.limiting_resource)
        for name, available in result.available.items():
            print(name, available.value, available.unit, available.status.value, available.reason)
        for assumption in result.assumptions:
            print("Assumption:", assumption)


if __name__ == "__main__":
    anyio.run(main)
