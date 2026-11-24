# Configuration

Pass a `ResourceConfig` explicitly. RESOURCERA does not read its own environment
variables; applications may parse their environment and pass validated settings.

```python
from resourcera import ResourceConfig, ResourceManager

config = ResourceConfig(
    cpu_memory_interval_seconds=2,
    disk_network_interval_seconds=10,
    inventory_interval_seconds=300,
    safety_margin=0.1,
    max_concurrency=8,
    max_waiters=32,
)
manager = ResourceManager(config)
```

The default attributable CPU budget is 1% of one logical CPU over 60 seconds;
estimated retained telemetry state is limited to 16 MiB. Bootstrap work has a
separately reported bounded allowance. These are control targets for attributable
work, not an operating-system CPU or memory quota.

Set `coordination_database` to an **absolute local file path** to enable
cooperating-process SQLite leases. Disk read/write and combined network capacity
must be configured explicitly before those throughput requirements can be admitted.

Adaptive concurrency controllers re-evaluate capacity every
`concurrency_adjustment_interval_seconds` (default 2). Each increase adds
`concurrency_increase_step` (default 1), and each controller keeps at most
`concurrency_history_size` adjustments (default 32). See
[Adaptive concurrency](../guide/concurrency.md).

`admission_wait_timeout_seconds` defaults to 5 seconds and
`concurrency_wait_timeout_seconds` defaults to `None` (no permit deadline).
Either can be configured as `None` for indefinite waits. Queue sizes are still
bounded. `admission_max_bypass` (default 8) limits how many later,
higher-priority requests can be inserted ahead of a queued request.
`admission_heartbeat_interval_seconds` (default 2, range 0.05–3600, `None` to
disable) sets how often queued admission requests record a liveness heartbeat;
heartbeats never admit a request or change its deadline. See
[Admission](../guide/admission.md) for per-call overrides, fairness and heartbeats.

Count fields (`max_provider_concurrency`, `max_concurrency`, `max_waiters`,
`admission_max_bypass`, `concurrency_increase_step`, `concurrency_history_size`)
must be real integers. Booleans, floats such as `2.0` and numeric strings such as
`"8"` are rejected rather than coerced. The same applies to the concurrency
bounds and `max_waiters` passed to `recommend_concurrency()`,
`adaptive_concurrency()` and `remote_concurrency()`, which raise `TypeError`.

See the [API reference](../api/index.md) and
[coordination guide](../guide/coordination.md).
