"""Define explicit profiles and demands. Requires: pip install resourcera."""

import anyio

from resourcera import ResourceManager, WorkloadProfile, WorkloadRequirements


async def main() -> None:
    async with ResourceManager() as resources:
        for profile in (*WorkloadProfile, "custom-batch"):
            request = WorkloadRequirements(memory="32MiB", profile=profile)
            estimate = await resources.capacity(request)
            print(profile, "estimated concurrency:", estimate.max_concurrency)


if __name__ == "__main__":
    anyio.run(main)
