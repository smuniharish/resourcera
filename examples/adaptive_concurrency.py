"""Run a fixed batch with resource-aware concurrency. Requires: pip install resourcera."""

import anyio

from resourcera import AdmissionRejectedError, ResourceConfig, ResourceManager


async def main() -> None:
    config = ResourceConfig(max_concurrency=2, max_waiters=4, admission_wait_timeout_seconds=3)
    async with ResourceManager(config) as resources:

        async def work(number: int) -> None:
            try:
                async with resources.admission({"memory": "8MiB"}, wait=True) as decision:
                    print("Admitted item:", number, decision.lease_id)
                    await anyio.sleep(0.05)
            except AdmissionRejectedError as error:
                print("Item rejected:", number, error.decision.reason)

        async with anyio.create_task_group() as tasks:
            for number in range(4):
                tasks.start_soon(work, number)
        result = await resources.capacity({"memory": "8MiB"})
        print("Current estimated concurrency:", result.max_concurrency)


if __name__ == "__main__":
    anyio.run(main)
