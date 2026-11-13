# Workloads, capacity, and coordination

Specify at least one measured demand using `WorkloadRequirements` or a mapping.
CPU is expressed in CPU equivalents; memory accepts values such as `"1GiB"`;
disk and network demands are bytes per second. Accelerator memory is per device.
Profile names label workloads but do not infer demands.

`await manager.capacity(requirements)` provides `max_concurrency`,
`limiting_resource`, and assumptions. `max_concurrency=None` means capacity is
unknown; zero means known capacity is exhausted. Disk and network capacity
estimates require explicit throughput ceilings in `ResourceConfig`.

`manager.admission(requirements)` is an async context manager. Rejection raises
`AdmissionRejectedError` with a decision. Waiting is off by default; `wait=True`
uses a finite timeout and bounded queue. Keep the context open until work finishes;
lease loss cancels protected work and surfaces `ReservationError`.

`await manager.recommend_concurrency(requirements, min_concurrency=..., max_concurrency=...)`
returns a bounded `recommended_limit` with its limiting resource, reason, and
confidence. Use it to size a `ThreadPoolExecutor`; there is no separate worker API.
`async with manager.adaptive_concurrency(requirements, ...) as controller` yields a
controller whose `slot()` holds a permit and a lease, and whose
`run_sync(executor, fn, *args)` releases capacity only after the callable finishes.
Limits drop immediately, rise slowly, hold while capacity is unknown, and never
cancel admitted work. `signal_overload()` halves the limit after external
feedback such as timeouts or HTTP 429. Inspect `controller.state` for the limit,
counts, reason, and bounded history.

`reserve`, `renew`, `reservation`, and `release` operate on standalone leases.
Default claims coordinate only the current interpreter. An explicit shared SQLite
database coordinates cooperating processes on the same machine and local
filesystem; it is not distributed coordination, authentication, or OS isolation.
Never assume nonparticipants honor a reservation.
