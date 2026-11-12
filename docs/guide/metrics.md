# Metrics and freshness

Each `Metric` includes a value, unit, source, scope, wall-clock observation time,
monotonic age, freshness window, measurement status, and optional detail or
confidence. Its `status` property incorporates current age.

| Status | Meaning |
| --- | --- |
| `available` | A usable observation was obtained. |
| `estimated` | A usable estimate with stated limitations. |
| `unavailable` | No value is currently available. |
| `unsupported` | The interface does not provide the metric. |
| `stale` | A once-usable value exceeded its freshness window or polling was suspended. |
| `error` | Collection or validation failed. |

Unavailable, unsupported, and error metrics have no value. A stale metric can retain
its old value for inspection but is not usable for admission. Counter rates require
two valid observations; resets and invalid deltas cause a new warm-up. A failed
latest system sample invalidates cached system capacity estimates.

Snapshots are immutable records, but CPU, memory, disk, network, and accelerator
observations may have different collection times. Do not equate snapshot age with
freshness of every metric.
