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
contention. Waiting has a finite timeout and a configured maximum number of
waiters. Within one manager, larger priority values go first and equal priorities
are FIFO; a stream of higher-priority requests can starve lower-priority work.
There is no cross-process priority or fairness guarantee.

AnyIO's capacity limiter bounds local work while the atomic resource policy
decides each admission. The effective allowance adapts to fresh capacity; already
admitted work is not cancelled merely because capacity decreases. A lost lease
does cancel its admission context and surfaces `ReservationError`.

To replace hardcoded semaphores, limiters, or thread-pool sizes with
resource-derived limits, see [Adaptive concurrency](concurrency.md).
