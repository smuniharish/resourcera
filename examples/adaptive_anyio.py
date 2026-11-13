"""Replace a fixed anyio.CapacityLimiter with adaptive limits. Requires: pip install resourcera."""

import anyio

from resourcera import AdmissionRejectedError, ResourceManager


async def main() -> None:
    async with (
        ResourceManager(scope="machine") as manager,
        manager.adaptive_concurrency(
            {"cpu": 0.1, "memory": "16MiB"},
            max_concurrency=8,
            max_waiters=32,
            wait_timeout_seconds=5,
        ) as controller,
    ):
        print("Initial limit:", controller.current_limit)
        completed = 0

        async def work(number: int) -> None:
            nonlocal completed
            try:
                async with controller.slot():
                    await anyio.sleep(0.01)
                    completed += 1
            except AdmissionRejectedError as error:
                print("Item rejected:", number, error.decision.reason)

        async with anyio.create_task_group() as tasks:
            for number in range(16):
                tasks.start_soon(work, number)
        print("Completed items:", completed)
        print("Current limit:", controller.current_limit)
        print("Reason:", controller.state.adjustment_reason)


if __name__ == "__main__":
    anyio.run(main)
