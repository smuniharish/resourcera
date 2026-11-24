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
latency, cooldown and bounded adjustment history. Permit waiters are served in
FIFO order.

## Adaptation window and low traffic

The controller keeps a bounded window of recent outcomes: at most `max_samples`
(default 256), each discarded once older than `sample_max_age_seconds` (default:
the larger of 30 seconds and four observation windows). Ages use the monotonic
clock. The limit increases by at most one per observation window, and only when
all of these hold:

- at least `observation_window_seconds` have passed since the last evaluation;
- the window holds at least `minimum_samples` fresh outcomes (successes plus
  reported failures);
- at least 90% of those outcomes were successes, and the window's mean latency
  and any reported remote queue wait have not risen materially above their
  slowly updated baselines; and
- no overload cooldown or Retry-After delay is active.

After each evaluation the window is cleared, so the next step needs new evidence.

Insufficient or stale feedback is never treated as health. With fewer than
`minimum_samples` fresh outcomes the limit holds, however much time passes.
An endpoint whose traffic is sparser than `minimum_samples` per
`sample_max_age_seconds` therefore stays at its current limit. Raise the maximum
age, or lower `minimum_samples` (2 at least), if such endpoints should adapt.
With no feedback at all, the limit stays where it is. A burst of thousands of
successes within one window still produces only one increase and keeps only the
newest `max_samples` outcomes in memory.

Decreases are immediate. Overload halves the limit (never below
`min_concurrency`) once per cooldown and restarts the window with the overload
as its only outcome, so recovery requires a fresh run of successes.
`report_capacity()` lowers the limit at once while requests are in flight; they
keep running and new requests wait until in-flight work drains below the new
limit.

## Classifying outcomes

Only congestion signals should be passed to `report_overload`: HTTP 429 or 503,
timeouts, and connection or network failures that indicate the endpoint is
saturated. Authentication, validation (`invalid_request`) and application errors
belong in `report_failure`; they count against the success ratio and block
increases but never decrease the limit. Do not report ordinary connection
failures, such as DNS errors or refused connections to a stopped service, as
overload: they do not indicate congestion. Explicit remote capacity hints belong
in `report_capacity`.

## Ownership, threads and reuse

A controller belongs to one event loop: the loop that was running when it was
created, otherwise the first event loop that uses it. Ownership is the identity of
that loop (and the thread running it), not merely the thread: a later event loop
on the same thread is a different owner. Binding is atomic: if several loops use
an unbound controller at once, exactly one becomes the owner and the others are
rejected without changing state. The owner never changes afterwards, even after
the owning loop ends.

Every operation is owner-only: feedback methods, entering and leaving `slot()`,
entering and leaving the controller context, and reading `remote.state`. They are
synchronous with respect to the owning loop and need no extra locking there.
Calls from any other thread or event loop, from outside an event loop, or after
the owning loop has ended raise `RuntimeError` without changing state. Before the
controller is bound, `state` may be read from anywhere; once bound, capture any
state you need before the owning loop ends. Create a new controller for each new
event loop. From a worker thread, marshal feedback onto the owning loop:

```python
anyio.from_thread.run_sync(lambda: remote.report_success(latency_seconds=elapsed))
```

Closing the manager from another thread or event loop is allowed: the controller
is marked closed immediately, and waiting slots are woken on the owning loop. If
the owning loop has already ended there is nothing to wake, and closing still
succeeds. After the controller or its
manager closes, every feedback method (`report_success`, `report_overload`,
`report_failure`, `report_capacity`) and `slot()` raises `ManagerStateError`
without changing counters or the limit; `remote.state.closed` is `True`. The
ownership check runs first, so a call from a foreign thread or loop raises
`RuntimeError` even after closure. Requests that are already in flight finish
normally and release their permits.

`manager.remote_concurrency()` creates a new, independent controller on every
call. Create one controller per endpoint, or per capacity domain such as a
gateway or an API key, when the application starts, and reuse it for every
request. A controller created per request would never observe more than one
outcome, so it could not adapt or protect the endpoint. Two controllers with the
same key do not share state; RESOURCERA keeps no hidden global registry. Use the
controller as an async context manager, or rely on manager shutdown, to close it.

Each instance coordinates only tasks using that instance on its event loop in one
process: it does not coordinate processes or machines. SQLite resource
reservations do not change that scope.

## Combining with local admission

Local admission protects this process's resources, and the remote controller
protects the endpoint. They are independent and nest naturally:

```python
async with (
    manager.admission({"memory": "64MiB"}, wait=True, wait_timeout_seconds=2),
    remote.slot(),
):
    response = await send_request()
```

Each context manager releases its own capacity on success, exceptions and
cancellation. If local admission succeeds but the remote wait times out or is
cancelled, the local lease is released as the exception leaves the outer block.
The two timeouts are independent: the admission timeout bounds the lease wait,
and the controller's `wait_timeout_seconds` bounds the remote permit wait.

Choose one acquisition order and use it throughout the application:

- **Local first, then remote** (recommended). The remote permit is held only
  while the request is actually sent. Use a finite remote `wait_timeout_seconds`
  so local resources are not reserved indefinitely while waiting for the
  endpoint.
- **Remote first, then local.** Local capacity is not held while waiting for the
  endpoint, but remote permits sit idle while waiting for local admission, which
  lowers the endpoint's effective concurrency.

The [composition example](../examples/index.md) (`local_remote_composition.py`)
demonstrates the recommended order.

## Rate limits
Concurrency limits in-flight requests; it does **not** enforce requests per
minute or token quotas. Compose with a separately chosen rate limiter when needed.
For example, [aiolimiter](https://aiolimiter.readthedocs.io/) supplies an asyncio
rate limiter; check its backend support before using it with Trio. RESOURCERA
does not install or include a rate-limiting dependency.
