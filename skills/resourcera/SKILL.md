---
name: resourcera
description: Use RESOURCERA snapshots, capacity estimates, admission, and leases safely.
---

# RESOURCERA

Use the public async API and close each manager:

```python
from resourcera import AdmissionRejectedError, ResourceManager

async with ResourceManager() as resources:
    snapshot = await resources.snapshot()
    estimate = await resources.capacity({"memory": "64MiB"})
    try:
        async with resources.admission({"memory": "64MiB"}) as decision:
            await do_work()
    except AdmissionRejectedError as error:
        print(error.decision.reason)
```

- Treat each metric's value, unit, source, status, and age together. `None`, stale,
  unsupported, and failed observations are not zero or unlimited.
- CPU and counter rates need warm-up. Estimates and utilization are not guarantees.
- Specify actual workload demands; profile names do not assign resources. Disk and
  network admission require explicit throughput ceilings.
- Admission rejects immediately unless bounded waiting is requested. Keep the
  admission context open throughout the work and handle rejection and lease loss.
- Default claims are interpreter-local. SQLite coordinates cooperating processes
  only on one machine and local filesystem; it is not resource isolation.
- Read the project's documentation and tests before using platform-specific or
  accelerator capabilities. Do not install SDKs or drivers automatically.
