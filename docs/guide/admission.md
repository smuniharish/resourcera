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
  lower-priority waiter, but each waiter accepts at most
  `ResourceConfig.admission_max_bypass` such insertions ahead of it (default 8).
  After that it is pinned: every later arrival queues behind it, whatever its
  priority.
- The budget is charged when the later request is **inserted** ahead, not when it
  is admitted. An overtaker that is later cancelled or times out still counts, so
  the bound is conservative: cancellations can only make a waiter pin sooner.
- `admission_max_bypass=0` gives plain arrival order.

A waiter is therefore admitted after at most the requests already ahead of it
plus `admission_max_bypass` inserted overtakers, provided capacity is eventually
released.
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

### Waiter liveness heartbeats

While a request is queued, its own waiting task records a liveness heartbeat
every `ResourceConfig.admission_heartbeat_interval_seconds` (2 seconds by
default, 0.05–3600; `None` disables heartbeats). Heartbeats exist so that long
and indefinite waits stay observable, for example by an application watchdog:

- A heartbeat means only "this request is still queued and its task is running".
  It never admits the request, never implies that resources are available, and
  never touches the queue order, priorities, permits or reservations.
- Heartbeats never change a deadline. A finite wait expires at the deadline
  computed once, with a monotonic clock, when it was enqueued. An indefinite wait
  (`wait_timeout_seconds=None`) has no deadline and heartbeats never give it one.
- Heartbeats stop as soon as the request is admitted, cancelled, times out, is
  rejected or is ended by manager shutdown. Finished waiters are not retained.

`admission_state()` exposes, for this manager only:

| Field | Meaning |
| --- | --- |
| `indefinite_waiter_count`, `finite_waiter_count` | Queued waits without and with a deadline. |
| `admitted_waiter_count`, `shutdown_waiter_count` | Cumulative queued requests that were admitted or ended by shutdown. |
| `heartbeat_interval_seconds` | Configured interval, or `None` when disabled. |
| `heartbeat_count`, `last_heartbeat_at` | Cumulative heartbeats; UTC time of the latest one. |
| `missed_heartbeat_count`, `max_heartbeat_delay_seconds` | Whole intervals skipped by late heartbeats, and the largest lateness in seconds. |
| `heartbeat_overdue_waiter_count` | Queued waiters whose last heartbeat (or enqueue time) is older than two intervals. |
| `waiters` | One `AdmissionWaiterState` per queued request: sequence, lease ID, priority, position, status, wait duration, timeout, remaining time and heartbeat statistics. |

Durations are monotonic seconds; `remaining_seconds` and `timeout_seconds` are
`None` for indefinite waits. Heartbeat fields stay zero or `None` when heartbeats
are disabled or have not occurred; values are never fabricated.

A late heartbeat is reported, not punished. When the event loop is blocked or
congested, the next heartbeat is coalesced into one, the skipped intervals are
counted in `missed_heartbeat_count`, and the waiter's `status` is
`"heartbeat_overdue"` until it runs again. An overdue waiter is still verified to
be in the queue: RESOURCERA never removes, admits or expires a waiter because a
heartbeat was late. Watchdogs should treat it as a symptom of loop congestion,
not as a dead request.

Heartbeats are cheap: they reuse each waiter's existing task (no extra tasks,
threads or registries), take no lock, never sample resources and only update a
few counters, so per-waiter state is constant-size and the `waiters` view is
bounded by `max_waiters`. The cost is one extra timer wake-up per waiter per
interval; raise the interval or set it to `None` for very large queues. These
heartbeats are process-local, like the queue itself: they say nothing about
waiters in other processes sharing a reservation store.

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
calls raise `ManagerStateError`, queued waiters are woken with `ManagerStateError` (ending
their heartbeats), and a lease whose transaction completes during shutdown is
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
