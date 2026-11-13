"""Run controllers that share one reservation ledger. Requires: pip install resourcera."""

import anyio

from resourcera import ConcurrencyController, ResourceConfig, ResourceManager


async def calibrated(controller: ConcurrencyController) -> None:
    # CPU utilization and throughput need two samples before capacity is measurable.
    for _ in range(20):
        if controller.state.confidence > 0:
            return
        await anyio.sleep(0.5)
        await controller.adjust()


async def main() -> None:
    config = ResourceConfig(
        network_capacity_bytes_per_second=125_000_000,
        cpu_memory_interval_seconds=0.5,
        disk_network_interval_seconds=1,
    )
    async with (
        ResourceManager(config) as manager,
        manager.adaptive_concurrency(
            {"cpu": 0.5, "profile": "cpu-bound"}, max_concurrency=8
        ) as cpu,
        manager.adaptive_concurrency(
            {"network_bytes_per_second": 1_000_000, "profile": "network-bound"},
            max_concurrency=100,
        ) as network,
    ):
        await calibrated(cpu)
        await calibrated(network)
        print("CPU controller limit:", cpu.current_limit, cpu.state.limiting_resource)
        print("Network controller limit:", network.current_limit, network.state.limiting_resource)

        # Every slot holds a real lease, so each controller's work is counted in shared capacity.
        async with cpu.slot(), network.slot():
            print("Active slots:", cpu.state.active_count + network.state.active_count)
        print("Active slots after release:", cpu.state.active_count + network.state.active_count)


if __name__ == "__main__":
    anyio.run(main)
