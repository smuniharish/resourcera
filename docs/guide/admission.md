# Admission and adaptive concurrency

`manager.admission(requirements)` returns an async context manager. Its accepted
decision contains the request, capacity estimate, limiting resource, freshness,
margin, policy, reason, and lease ID. Insufficient or unknown capacity raises
`AdmissionRejectedError`, whose `decision` explains the rejection.

```python
from resourcera import AdmissionRejectedError

try:
    async with manager.admission(
        {"cpu": 0.5, "memory": "1GiB", "priority": 5},
        wait=True,
        wait_timeout_seconds=3,
    ) as decision:
        await do_work()
except AdmissionRejectedError as error:
    print(error.decision.reason)
```

Without `wait=True`, rejection is immediate apart from bounded local-store
contention. With waiting enabled, omitting `wait_timeout_seconds` uses
`ResourceConfig.admission_wait_timeout_seconds` (5 seconds by default); passing
`None` explicitly waits indefinitely, subject to cancellation or manager shutdown.
Positive finite values specify a deadline; zero and negative values are invalid.
The admission queue remains bounded by `ResourceConfig.max_waiters`, including
for indefinite waits. Within one manager, larger priority values go first and
equal priorities are FIFO; a stream of higher-priority requests can starve
lower-priority work. There is no cross-process priority or fairness guarantee.

Explicit `None` previously selected the configured timeout; it now means
indefinite waiting. Omit the argument to retain the former behavior.

`await manager.admission_state()` reports the manager-local queue capacity,
depth, oldest wait, priority distribution, owned reservations, rejections,
timeouts and cancellations. Its completed-wait count and total wait duration
include completed, timed-out and cancelled queued requests. It is not a
cross-process view of SQLite reservations.

Instrumentation such as coverage or profiling can increase measured library
CPU cost and cause the strict overhead policy to suspend admission. Check the
rejection reason and overhead state when this happens; a finite admission wait
may expire before the overhead window recovers.

For large inputs, use a bounded producer queue and a fixed number of workers
instead of creating one task per item. The
[bounded producer example](../examples/index.md) uses an AnyIO memory stream:
the producer pauses when the buffer fills.

For memory-heavy work, estimate each operation's peak working memory from
representative measurements, including native allocations and temporary
buffers. Input file size and process RSS alone are not reliable per-operation
peak estimates. Reserve that amount for the entire memory-consuming operation
and release it through the admission context, including on failure or
cancellation.

Reservations are logical claims, not operating-system memory isolation. The
shared reservation store accounts for cooperating managers in its configured
scope, but does not provide atomic per-session and global hierarchical budgets.
Applications needing both must coordinate the session budget separately; two
independently acquired limits are not an atomic hierarchical reservation.

AnyIO's capacity limiter bounds local work while the atomic resource policy
decides each admission. The effective allowance adapts to fresh capacity; already
admitted work is not cancelled merely because capacity decreases. A lost lease
does cancel its admission context and surfaces `ReservationError`.

To replace hardcoded semaphores, limiters, or thread-pool sizes with
resource-derived limits, see [Adaptive concurrency](concurrency.md).
