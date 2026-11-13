"""Replace a fixed asyncio.Semaphore with adaptive concurrency. Requires: pip install resourcera."""

import asyncio

from resourcera import ResourceManager


async def handle(number: int) -> int:
    await asyncio.sleep(0.01)
    return number * 2


async def main() -> None:
    async with (
        ResourceManager(scope="machine") as manager,
        manager.adaptive_concurrency({"memory": "32MiB"}, max_concurrency=20) as controller,
    ):
        print("Initial limit:", controller.current_limit)

        async def guarded(number: int) -> int:
            async with controller.slot():
                return await handle(number)

        results = await asyncio.gather(*(guarded(number) for number in range(30)))
        print("Handled items:", len(results))
        print("Limiting resource:", controller.state.limiting_resource)


if __name__ == "__main__":
    asyncio.run(main())
