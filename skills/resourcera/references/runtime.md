# Runtime, metrics, and lifecycle

- `async with ResourceManager(...) as manager` starts sampling and closes owned
  tasks, providers, and leases when the context exits. The manager must outlive
  every admitted workload.
- `await manager.snapshot()` returns a cached immutable observation. Check each
  `Metric`'s `value`, `unit`, `source`, `scope`, and current `status`; metrics in
  one snapshot can have different observation times.
- `MetricStatus.AVAILABLE` and `ESTIMATED` describe usable values with their
  limitations. `UNAVAILABLE`, `UNSUPPORTED`, `ERROR`, and `STALE` must not be
  silently converted to zero. A stale metric can retain its previous value.
- CPU utilization and disk/network rates need two samples. Sampling can be
  reduced or suspended by overhead policy, and required observations may become
  stale; inspect `snapshot.overhead` when diagnosing rejections.
- Machine, process, and container scopes have different meanings. See the current
  [architecture guide](https://resourcera.readthedocs.io/en/latest/guide/architecture/)
  for supported scopes and platform limitations.

The offline `scripts/verify_setup.py` checks that a manager can obtain a snapshot;
it does not require every platform metric or accelerator to be available.
