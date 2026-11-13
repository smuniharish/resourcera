"""Size a ThreadPoolExecutor and bound its submissions. Requires: pip install resourcera."""

import time
from concurrent.futures import ThreadPoolExecutor

import anyio

from resourcera import ResourceManager


def blocking_read(number: int) -> str:
    time.sleep(0.02)
    return f"item-{number}"


async def main() -> None:
    async with ResourceManager(scope="machine") as manager:
        requirements = {"memory": "64MiB", "profile": "io-bound"}
        recommendation = await manager.recommend_concurrency(
            requirements, min_concurrency=1, max_concurrency=16
        )
        print("Recommended workers:", recommendation.recommended_limit)
        print("Limiting resource:", recommendation.limiting_resource)
        print("Reason:", recommendation.reason)

        # The application owns the executor; RESOURCERA never shuts it down.
        with ThreadPoolExecutor(max_workers=recommendation.recommended_limit) as executor:
            async with manager.adaptive_concurrency(
                requirements, max_concurrency=recommendation.recommended_limit
            ) as controller:
                results: list[str] = []

                async def submit(number: int) -> None:
                    results.append(await controller.run_sync(executor, blocking_read, number))

                async with anyio.create_task_group() as tasks:
                    for number in range(8):
                        tasks.start_soon(submit, number)
                print("Completed submissions:", len(results))
                print("Permits in use after completion:", controller.state.active_count)


if __name__ == "__main__":
    anyio.run(main)
