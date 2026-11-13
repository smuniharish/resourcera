"""Bound network operations by a declared bandwidth budget. Requires: pip install resourcera."""

import anyio

from resourcera import ResourceConfig, ResourceManager

# Interface bandwidth alone does not determine safe request concurrency. Estimate each
# operation's bandwidth from its payload size and expected duration, and keep the
# application's own connection-pool or API rate limits as the maximum.
BYTES_PER_OPERATION = 2_000_000
EXPECTED_SECONDS = 0.5
CONNECTION_POOL_LIMIT = 50


async def fake_request(number: int) -> int:
    await anyio.sleep(0.01)
    return number


async def main() -> None:
    config = ResourceConfig(
        network_capacity_bytes_per_second=125_000_000, disk_network_interval_seconds=1
    )
    requirements = {"network_bytes_per_second": BYTES_PER_OPERATION / EXPECTED_SECONDS}
    async with (
        ResourceManager(config) as manager,
        manager.adaptive_concurrency(
            requirements, max_concurrency=CONNECTION_POOL_LIMIT
        ) as controller,
    ):
        # Throughput needs two samples; until then the controller stays at the minimum.
        for _ in range(20):
            if controller.state.confidence > 0:
                break
            await anyio.sleep(0.5)
            await controller.adjust()
        state = controller.state
        print("Network-limited concurrency:", state.current_limit)
        print("Limiting resource:", state.limiting_resource)

        async def request(number: int) -> None:
            async with controller.slot():
                await fake_request(number)

        async with anyio.create_task_group() as tasks:
            for number in range(10):
                tasks.start_soon(request, number)

        # Feed back remote signals such as timeouts or HTTP 429 responses.
        controller.signal_overload("The remote service returned HTTP 429.")
        print("Limit after overload feedback:", controller.current_limit)


if __name__ == "__main__":
    anyio.run(main)
