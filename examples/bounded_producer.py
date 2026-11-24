"""Use a bounded producer queue before resource-aware workers."""

import anyio

from resourcera import ResourceConfig, ResourceManager


async def main() -> None:
    config = ResourceConfig(max_concurrency=2, max_waiters=2)
    async with ResourceManager(config) as manager:
        send, receive = anyio.create_memory_object_stream[int](4)

        async def worker(items: anyio.abc.ObjectReceiveStream[int]) -> None:
            async with items:
                async for item in items:
                    async with manager.admission({"memory": "1MiB"}, wait=True):
                        await anyio.sleep(0.01)
                        print(f"processed {item}")

        async with anyio.create_task_group() as tasks:
            for _ in range(2):
                tasks.start_soon(worker, receive.clone())
            await receive.aclose()
            async with send:
                for item in range(12):
                    await send.send(item)


if __name__ == "__main__":
    anyio.run(main)
