"""Inspect the strict feedback budget. Requires: pip install resourcera."""

import anyio

from resourcera import ResourceConfig, ResourceManager


async def main() -> None:
    config = ResourceConfig(cpu_budget_fraction=0.01, managed_state_budget_bytes=16 * 1024 * 1024)
    async with ResourceManager(config) as resources:
        overhead = (await resources.snapshot()).overhead
        print("Policy:", resources.config.overhead_policy)
        print("State:", overhead.state)
        print("Measured CPU:", overhead.cpu_seconds.value, overhead.cpu_seconds.unit)
        print("Budget CPU:", overhead.cpu_budget_seconds, "seconds per window")
        print("Managed bytes:", overhead.managed_state_bytes.value)
        print("Disk attribution:", overhead.disk_activity_bytes.status.value)


if __name__ == "__main__":
    anyio.run(main)
