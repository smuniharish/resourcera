"""Read CPU utilization after warm-up. Requires: pip install resourcera."""

import anyio

from resourcera import ResourceConfig, ResourceManager


async def main() -> None:
    config = ResourceConfig(cpu_memory_interval_seconds=0.25)
    async with ResourceManager(config) as resources:
        await anyio.sleep(0.4)
        cpu = (await resources.snapshot()).cpu
        for label, reading in (
            ("Aggregate", cpu.utilization_percent),
            ("Per core", cpu.per_core_utilization_percent),
            ("Process", cpu.process_utilization_percent),
            ("Affinity", cpu.affinity_cpus),
            ("Cgroup headroom", cpu.cgroup_headroom_cores),
            ("Runnable tasks", cpu.runnable_tasks),
        ):
            print(label, reading.value, reading.unit, reading.status.value)


if __name__ == "__main__":
    anyio.run(main)
