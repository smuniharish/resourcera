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
for indefinite waits.

### Fairness

Within one manager the queue uses **bounded bypass**:

- Equal priorities are served in arrival order (FIFO).
- A later request with a larger priority value may move ahead of an earlier,
  lower-priority waiter, but each waiter can be overtaken at most
  `ResourceConfig.admission_max_bypass` times (default 8). After that it is
  pinned: every later arrival queues behind it, whatever its priority.
- `admission_max_bypass=0` gives plain arrival order.

A waiter is therefore admitted after at most the requests already ahead of it
plus `admission_max_bypass` overtakers, provided capacity is eventually released.
There is no time-based priority aging.

Only the request at the head of the queue is evaluated. A large queued request
holds back smaller requests behind it until it fits, times out or is cancelled.
This head-of-line blocking is deliberate: letting smaller requests fit around it
would starve large requests. Non-waiting requests are rejected while any request
is queued. Cancelled and timed-out waiters leave the queue immediately, and
admission never exceeds capacity or `max_concurrency`. There is no cross-process
priority or fairness guarantee.

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

## Shutdown

`await manager.close()` is idempotent and safe to call concurrently; later calls
wait for the first to finish. Once closing begins, new admission and reservation
calls raise `ManagerStateError`, queued waiters are woken with
`ManagerStateError`, and a lease whose transaction completes during shutdown is
released instead of being returned. Active admission contexts are cancelled and
their leases released; the manager's background tasks are stopped before
`close()` returns.

## Blocking work in threads

Running blocking work inside an admission context keeps the lease for the whole
call. `anyio.to_thread.run_sync` waits for the thread by default, so a
cancellation is delivered only after the function returns and the lease is not
released while the thread still uses resources. Python cannot forcibly stop a
running thread or native call; long blocking functions should check their own
stop condition. Avoid `abandon_on_cancel=True` inside an admission context: it
would release the lease while the abandoned thread keeps running.
`ConcurrencyController.run_sync` follows the same rule for executors.

To replace hardcoded semaphores, limiters, or thread-pool sizes with
resource-derived limits, see [Adaptive concurrency](concurrency.md).
