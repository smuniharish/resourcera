# Quickstart

```python
import anyio

from resourcera import AdmissionRejectedError, ResourceManager


async def main():
    async with ResourceManager(scope="machine") as manager:
        snapshot = await manager.snapshot()
        print(snapshot.memory.available_bytes.value)
        print(snapshot.memory.available_bytes.status)

        estimate = await manager.capacity({"memory": "64MiB"})
        print(estimate.max_concurrency, estimate.limiting_resource)

        try:
            async with manager.admission({"memory": "64MiB"}) as decision:
                print("Accepted lease:", decision.lease_id)
                await anyio.sleep(0.01)
        except AdmissionRejectedError as error:
            print("Rejected:", error.decision.reason)


anyio.run(main)
```

The context starts and closes all owned resources. Admission is immediate by
default; it can reject during sampling warm-up, resource pressure, or a suspended
overhead budget. `snapshot()` reads a cached observation, not a fresh hardware
scan. See [admission](../guide/admission.md) for waiting and cancellation.
