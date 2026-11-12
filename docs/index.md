# RESOURCERA

Understand local resources, estimate workload capacity, and admit bounded work
without coupling your application to a framework.

![Resource intelligence architecture](assets/diagrams/overall-architecture.png)

- [Install](getting-started/installation.md) and try the
  [quickstart](getting-started/quickstart.md).
- Learn how [measurement freshness](guide/metrics.md),
  [capacity](guide/capacity.md), and [admission](guide/admission.md) work.
- Review the exact [coordination guarantees](guide/coordination.md) and
  [overhead limitations](guide/sampling-overhead.md).

Measurements are observations, not promises. Missing or stale required data is
never treated as free capacity. Logical reservations are not physical isolation.
