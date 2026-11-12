# RESOURCERA

Resource intelligence, capacity estimation, and resource-aware concurrency for Python
applications.

RESOURCERA is an embeddable, async-first library for observing local resources,
estimating multidimensional workload capacity, and coordinating logical admission.
It supports Python 3.12+, asyncio and Trio, and does not require an accelerator SDK.
It is not a distributed execution engine, dashboard, or isolation mechanism.

## Install

From a checkout:

```shell
python -m pip install .
```

Once a distribution is published, the intended package name is `resourcera`. NVIDIA
telemetry is optional: install the `nvidia` extra only where NVML is needed.
The library never installs SDKs or drivers itself.

## Quickstart

```python
import anyio

from resourcera import AdmissionRejectedError, ResourceManager


async def main():
    async with ResourceManager(scope="machine") as resources:
        snapshot = await resources.snapshot()
        print("Available memory:", snapshot.memory.available_bytes.value, "bytes")

        try:
            async with resources.admission({"memory": "64MiB"}) as decision:
                print("Admitted:", decision.lease_id)
                await anyio.sleep(0.01)  # Your workload goes here.
        except AdmissionRejectedError as error:
            print("Not admitted:", error.decision.reason)


anyio.run(main)
```

CPU and throughput rates require two samples. Missing, stale, or failed required
measurements reject admission rather than becoming zero or unlimited capacity.
Reservations only coordinate cooperating participants; they do not enforce OS
isolation or guarantee future resource availability.

See the [documentation](https://resourcera.readthedocs.io/), the
[examples](examples/), and the [contribution guide](CONTRIBUTING.md). The project is
licensed under the [Apache License 2.0](LICENSE).
