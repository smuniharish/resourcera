# Adaptive concurrency

Many applications hardcode concurrency, for example
`ThreadPoolExecutor(max_workers=10)`, `asyncio.Semaphore(100)`, or
`anyio.CapacityLimiter(20)`. RESOURCERA replaces those guesses with limits
derived from the same snapshots, capacity estimates, and reservations used by
admission:

**Resource intelligence → capacity estimate → concurrency recommendation →
resource-aware admission.**

Two `ResourceManager` methods make up the API:

- `recommend_concurrency(requirements, *, min_concurrency=1, max_concurrency=None)`
  returns a one-off `ConcurrencyRecommendation`.
- `adaptive_concurrency(requirements, *, min_concurrency=1, max_concurrency=None,
  max_waiters=None, wait_timeout_seconds=None)` is an async context manager that
  yields a `ConcurrencyController`.

`max_concurrency` defaults to `ResourceConfig.max_concurrency`. A recommendation
or limit never leaves the `[min_concurrency, max_concurrency]` range.

## Recommendations

```python
recommendation = await manager.recommend_concurrency(
    {"cpu": 1.0, "memory": "512MiB"},
    min_concurrency=1,
    max_concurrency=32,
)
print(recommendation.recommended_limit)
print(recommendation.limiting_resource)
print(recommendation.reason)
```

The result reports:

- the recommended limit and bounds;
- the raw `estimated_limit`;
- `capacity_known`;
- the limiting resource;
- the full `CapacityResult`;
- the snapshot ID and age;
- the applied safety margin;
- assumptions;
- a reason;
- `confidence` between 0 and 1.

Confidence is the weakest measurement quality across the required dimensions. It
is halved when the overhead policy has reduced monitoring detail.

A recommendation is advice, not a guarantee. Another workload, or another
process that shares the coordination database, can claim the capacity before you
use it.

### Workers for a ThreadPoolExecutor

There is no separate worker API: a worker count is a concurrency
recommendation for the workload that the threads run. Describe what each task
needs, and optionally label it with a profile such as `io-bound`, `cpu-bound`,
`memory-bound`, `network-bound`, `disk-bound`, `gpu-bound`, or `mixed`.

```python
recommendation = await manager.recommend_concurrency(
    {"memory": "64MiB", "profile": "io-bound"},
    max_concurrency=32,
)
executor = ThreadPoolExecutor(max_workers=recommendation.recommended_limit)
```

A profile describes the workload category; it never invents resource demands.
For I/O-bound threads, CPU cores alone do not limit the count. Declare the memory,
disk, or network each task uses instead. RESOURCERA never applies a
`cores * 2` style rule.

## Adaptive controllers

```python
async with manager.adaptive_concurrency(
    {"cpu": 0.5, "memory": "256MiB"},
    min_concurrency=1,
    max_concurrency=50,
) as controller:
    async with controller.slot():
        await execute_workload()
```

Each `slot()` first takes a permit from an AnyIO `CapacityLimiter`, which
controls the controller's local limit. It then acquires a real logical lease
through `manager.admission(..., wait=True)`. Because every slot holds a lease:

- controllers for different workloads share one reservation ledger;
- no controller bypasses or double-counts the others;
- the global `ResourceConfig.max_concurrency` and cross-process coordination
  still apply.

On exit, the slot releases the lease and then the permit. Slots are single-use.

A background task re-evaluates capacity every
`concurrency_adjustment_interval_seconds`. It uses the cached snapshot and the
existing capacity estimate; acquiring a slot never triggers resource discovery.
You can also call `await controller.adjust()` yourself.

### Adjustment policy

The policy is deterministic and explainable:

- **Initial limit.** The first evaluation sets the limit directly. If capacity is
  not yet measurable, the limit stays at the minimum. The first measurable
  estimate then *calibrates* the limit directly instead of probing upward one step
  at a time.
- **Decreases.** These are immediate. When the target falls below the active
  count, admitted work keeps running, and new slots wait until active work drains
  below the limit. RESOURCERA never cancels work merely because a limit falls.
- **Increases.** An increase needs a smoothed (EWMA) target above the current
  limit for two consecutive evaluations, at least one adjustment interval since
  the last change, fresh measurements, and monitoring that is not degraded. Each
  increase adds `concurrency_increase_step`.
- **Unknown or stale capacity.** The limit is held; it is never treated as
  unlimited. Admission rejects work that needs an unmeasurable dimension, so slots
  fail with `AdmissionRejectedError` until the measurement is usable.
- **Overload feedback.** `controller.signal_overload(reason)` halves the limit
  immediately, for example after timeouts or HTTP 429 responses. This is the
  multiplicative-decrease half of the AIMD policy.

Because capacity counts the controller's own leases, the target is the active
count plus the additional operations that still fit. Availability is floored at
zero, so if capacity collapses while work runs, the limit first drops to the
active count and then converges as work finishes. Use `signal_overload` if you
need to fall below the active count sooner.

### Backpressure, cancellation, and shutdown

- At most `max_waiters` tasks wait for a permit; it defaults to
  `ResourceConfig.max_waiters`. Additional callers are rejected with
  `AdmissionRejectedError`. Waiters are served in FIFO order.
- `wait_timeout_seconds` bounds the wait for a permit. Admission also waits up to
  `admission_wait_timeout_seconds` for the lease.
- Cancelling a waiter leaves no permit or lease behind.
- Leaving the `adaptive_concurrency` block closes the controller. Pending waiters
  receive `ManagerStateError`, and running slots finish normally.
- Closing the manager cancels admission contexts that are still active. Their
  leases are released, and the controller closes.
- A lost lease cancels its slot and raises `ReservationError`.

## ThreadPoolExecutor submissions

`ThreadPoolExecutor` has no public way to resize its workers after construction,
and RESOURCERA does not touch its private internals. Instead, size the executor
from a recommendation and bound *submissions* with a controller:

```python
with ThreadPoolExecutor(max_workers=recommendation.recommended_limit) as executor:
    async with manager.adaptive_concurrency(
        {"memory": "64MiB"}, max_concurrency=recommendation.recommended_limit
    ) as controller:
        result = await controller.run_sync(executor, blocking_function, argument)
```

`run_sync` submits only while holding a slot, so the executor's queue cannot grow
without bound. The permit and lease are released only after the submitted
callable has finished:

- If the caller is cancelled while the callable is still queued, the future is
  cancelled.
- If the callable is already running, the cancellation is delivered after it
  completes, so capacity is never released while a thread is still working.
- Exceptions raised by the callable propagate to the caller.
- `run_sync` never shuts the executor down. If the executor is already shut down,
  `submit` raises `RuntimeError` and the slot is released.

### Changing the physical worker count

When a recommendation changes substantially and you need a different number of
threads, rotate executors rather than mutating one:

1. Create a new executor with the new `max_workers`.
2. Route new `run_sync` calls to the new executor.
3. Call `old.shutdown(wait=True, cancel_futures=False)` from a worker thread, for
   example with `anyio.to_thread.run_sync`, so the event loop is not blocked.
   Work that is already running or queued on the old executor completes, and each
   slot releases its permit when its own work finishes.
4. Only the owner shuts down an executor, and RESOURCERA never does.

The controller's limit continues to bound total submissions across both
executors during the rotation.

## asyncio and AnyIO

Controllers use AnyIO, so they work in asyncio programs and under AnyIO's
backends. Replace a fixed semaphore or limiter with a slot:

```python
async def guarded(item):
    async with controller.slot():
        return await handle(item)


results = await asyncio.gather(*(guarded(item) for item in items))
```

## Network-bound work

Interface bandwidth does not determine safe request concurrency. Request
concurrency also depends on payload sizes, latency, remote performance,
connection pools, rate limits, and retries. RESOURCERA does not guess these, and
it has no HTTP-client integration. Instead:

- configure `network_capacity_bytes_per_second`, because interface link speed is
  not a reliable ceiling;
- derive the per-operation requirement from your own estimate, for example
  `bytes_per_operation / expected_seconds`;
- keep the connection-pool or API rate limit as `max_concurrency`;
- feed back remote signals with `signal_overload`.

```python
config = ResourceConfig(network_capacity_bytes_per_second=125_000_000)
async with (
    ResourceManager(config) as manager,
    manager.adaptive_concurrency(
        {"network_bytes_per_second": 2_000_000 / 0.5},
        max_concurrency=50,
    ) as controller,
):
    async with controller.slot():
        await make_request()
```

Without a configured ceiling, network capacity is unknown, so the controller
stays at its minimum. A recommendation's assumptions also report the observed
connection count when `sample_process_connections` is enabled.

## Disk-bound work

Disk throughput ceilings are not portable, so configure
`disk_read_capacity_bytes_per_second` and `disk_write_capacity_bytes_per_second`.
Available throughput is the ceiling minus observed traffic, scaled by I/O
pressure where the platform reports it, after the safety margin and claims.
Queue depth and latency are recorded as assumptions when exposed and are never
fabricated.

```python
async with manager.adaptive_concurrency(
    {
        "disk_read_bytes_per_second": 50_000_000,
        "disk_write_bytes_per_second": 20_000_000,
        "profile": "disk-bound",
    },
    max_concurrency=40,
) as controller:
    async with controller.slot():
        await process_file(path)
```

## Observing a controller

`controller.state` returns an immutable `ConcurrencyState` without performing
any measurement. It contains:

- the current and recommended limits;
- the bounds;
- the active and pending counts;
- available permits;
- the last evaluation and adjustment times;
- the limiting resource and adjustment reason;
- the last capacity estimate;
- confidence and snapshot age;
- CPU, memory, and I/O pressure;
- whether the controller is closed;
- a bounded `history` of `ConcurrencyAdjustment` records, sized by
  `concurrency_history_size`.

Limit changes are logged at `INFO` on the `resourcera` logger.

## Overhead

Controllers reuse cached snapshots and capacity calculations. Adjustments run at
most once per interval, while waiter queues, history, and background tasks (one
per controller) are bounded. Measurement detail follows the strict overhead
policy. When that policy degrades monitoring, controllers hold their limits
instead of increasing.
