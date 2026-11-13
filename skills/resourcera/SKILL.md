---
name: resourcera
description: >-
  Use this skill when adding or debugging RESOURCERA resource snapshots, capacity
  estimates, admission, reservations, or adaptive concurrency limits in a Python
  application. Do not use it to
  implement a custom accelerator provider; use resourcera-provider instead.
license: Apache-2.0
metadata:
  author: S MUNI HARISH
  version: "0.2.0"
---

# Using RESOURCERA

RESOURCERA observes local resources and provides conservative workload admission.
Use its public async API; preserve the application's existing behavior and
framework.

## Workflow

1. Inspect the application's lifecycle, Python environment, actual resource
   requirements, and existing tests. Do not install dependencies without the
   user's approval.
2. Run `python <skill directory>/scripts/verify_setup.py` in the application's
   Python environment. Add `--json` for a machine-readable report. The check is
   offline, opens and closes one manager, and reports unavailable metrics rather
   than inventing values.
3. Keep a `ResourceManager` alive for the lifetime of the work. Start it with an
   async context manager or pair `start()` with `close()` in `finally`.
4. Describe actual workload demands, inspect the estimate and its limiting
   resource, and handle unknown capacity explicitly. Profile names alone do not
   supply demands.
5. Hold the admission context throughout the work, handle
   `AdmissionRejectedError`, and account for `ReservationError` if a lease is lost.
   Waiting must be explicitly requested and bounded.
6. To replace a hardcoded semaphore, limiter, or executor size, use
   `recommend_concurrency` for a one-off limit or `adaptive_concurrency` with
   `controller.slot()` / `controller.run_sync(executor, ...)` for adjusting
   limits. Keep application limits such as connection pools as `max_concurrency`.
7. Run focused application tests, including rejection, cancellation, and cleanup.
   Re-run the setup check when practical.

## Minimal example

```python
import anyio

from resourcera import AdmissionRejectedError, ResourceManager


async def main():
    async with ResourceManager() as resources:
        snapshot = await resources.snapshot()
        print("Memory status:", snapshot.memory.available_bytes.status)
        estimate = await resources.capacity({"memory": "64MiB"})
        print("Capacity:", estimate.max_concurrency, estimate.limiting_resource)
        try:
            async with resources.admission({"memory": "64MiB"}) as decision:
                print("Admitted:", decision.lease_id)
                await anyio.sleep(0.01)
        except AdmissionRejectedError as error:
            print("Not admitted:", error.decision.reason)


anyio.run(main)
```

## Non-negotiable boundaries

- Read a metric's value together with its status and freshness. Unavailable,
  unsupported, stale, and failed measurements are not zero or unlimited capacity.
- CPU and counter rates need warm-up; throughput admission requires configured
  ceilings. Estimates are observations, not guarantees.
- Default claims are interpreter-local. SQLite coordinates cooperating processes
  on one machine and a local filesystem only. Neither leases nor estimates provide
  physical isolation or control nonparticipants.
- Device visibility does not prove accessibility or available compute. Accelerator
  memory is per device, not a pooled value. Never install SDKs or drivers silently.
- A standalone lease must be released and monitored or renewed; a lost lease must
  stop the protected work. Do not create unbounded retry loops.
- Concurrency limits stay at the minimum while capacity is unknown; never bypass
  them with a fixed fallback. Network bandwidth alone does not determine request
  concurrency. Do not mutate `ThreadPoolExecutor` internals.

## References

- [Runtime, metrics, and lifecycle](references/runtime.md)
- [Workloads, capacity, and coordination](references/workloads.md)
