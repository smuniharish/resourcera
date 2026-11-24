"""Use independent feedback controllers for two simulated gateways."""

import time

import anyio

from resourcera import RemoteConcurrencyController, ResourceManager


async def main() -> None:
    async with (
        ResourceManager() as manager,
        manager.remote_concurrency("gateway-a", initial_concurrency=2, max_concurrency=8) as first,
        manager.remote_concurrency("gateway-b", initial_concurrency=2, max_concurrency=8) as second,
    ):

        async def call(controller: RemoteConcurrencyController, index: int) -> None:
            async with controller.slot():
                started = time.monotonic()
                await anyio.sleep(0.01)
                if controller.key == "gateway-a" and index == 0:
                    controller.report_overload(status_code=429)
                else:
                    controller.report_success(latency_seconds=time.monotonic() - started)

        async with anyio.create_task_group() as tasks:
            for index in range(8):
                tasks.start_soon(call, first, index)
                tasks.start_soon(call, second, index)
        print(first.state.key, first.state.current_limit, first.state.overload_count)
        print(second.state.key, second.state.current_limit, second.state.overload_count)


if __name__ == "__main__":
    anyio.run(main)
