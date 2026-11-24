# Remote service concurrency

Remote service feedback is independent of local CPU, memory, network throughput
and resource capacity. Create one controller for each endpoint and report actual
outcomes; without feedback, its explicitly configured initial limit remains fixed.

```python
async with manager.remote_concurrency(
    "gateway-primary", initial_concurrency=3, max_concurrency=32
) as remote:
    async with remote.slot():
        started = time.monotonic()
        response = await send_request()
        if response.status_code in (429, 503):
            remote.report_overload(
                status_code=response.status_code,
                retry_after=response.headers.get("Retry-After"),
            )
        elif response.is_success:
            remote.report_success(latency_seconds=time.monotonic() - started)
        else:
            remote.report_failure(kind="application_error")
```

`report_overload` accepts HTTP 429 or 503, or an explicit timeout, connection
failure or network failure reason. `Retry-After` must be integer seconds or an
HTTP date, at most 24 hours in the future; malformed values raise `ValueError`.
New requests wait for the cooldown and valid Retry-After delay to expire.
Repeated overload signals during a cooldown count separately but cause only one
multiplicative decrease. A healthy observation window with enough successes and
stable smoothed latency permits one additive increase. Authentication, invalid
requests and application failures are reported separately and do not halve the
limit. `report_capacity()` sets or clears an application-supplied ceiling;
decreases apply immediately and increases remain gradual.

When a service reports its own queue wait, pass it as
`remote_queue_wait_seconds` to `report_success`. Increasing smoothed queue wait
can hold the limit even when latency is otherwise stable. This is separate from
time spent waiting for a local controller permit; omit it when unavailable.

An in-flight request is not cancelled when the limit falls. The queue is bounded
by `max_waiters`; extra callers raise `RemoteConcurrencyRejectedError`. Explicit
positive `wait_timeout_seconds` bounds waiting, including cooldown; `None` waits
until admission, cancellation or shutdown. `remote.state` exposes counters,
latency, cooldown and bounded adjustment history.

Each instance has independent state, even if its key matches another instance.
It coordinates only tasks using that instance on its event loop in one process:
it does not coordinate processes or machines. SQLite resource reservations do not
change that scope. To combine remote and local control, nest `remote.slot()` and
`manager.admission(...)`, choosing a consistent acquisition order throughout
your application.

Concurrency limits in-flight requests; it does **not** enforce requests per
minute or token quotas. Compose with a separately chosen rate limiter when needed.
For example, [aiolimiter](https://aiolimiter.readthedocs.io/) supplies an asyncio
rate limiter; check its backend support before using it with Trio. RESOURCERA
does not install or include a rate-limiting dependency.
