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

See the [API reference](../api/index.md) and
[coordination guide](../guide/coordination.md).
