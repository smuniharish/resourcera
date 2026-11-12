# Reservations and coordination

`await manager.reserve(requirements)` returns a `ResourceLease`.
`await manager.renew(lease)`, `await manager.reservation(lease)`, and
`await manager.release(lease)` renew, inspect, and release it. Release is
idempotent. Supplying the same `lease_id` and requirements while a lease is active
returns the same lease; a conflicting owner or request is rejected.

Leases have an owner, ID, expiration, and optional automatic renewal. One bounded
supervisor renews a manager's active leases. Set `auto_renew=False` only if the
application will renew before expiry. Expired leases are not revived. The reported
wall-clock expiration is informational; enforcement uses the system-wide monotonic
clock and an OS boot identity.

By default, claims are shared within the current Python interpreter. For cooperating
processes on the **same machine and local filesystem**, set every participant's
`coordination_database` to the same absolute SQLite file. SQLite transactions
atomically check claims and insert or release leases. Participants with active
leases must agree on coordination policy and throughput budgets. A crashed owner
is recovered after its lease expires; a verified new boot invalidates prior leases.
If a reliable boot identity cannot be read, SQLite coordination fails closed.

This is not a distributed backend, an authentication boundary, or physical resource
isolation. Do not use a network filesystem, copy a live database between machines,
or allow untrusted writers. Nonparticipants are not coordinated. Process suspension,
clock/OS failures, and lost renewals can cause lease loss; applications using
standalone leases must check or renew them and stop work on loss. Keep the manager
open for the entire workload lifetime.
