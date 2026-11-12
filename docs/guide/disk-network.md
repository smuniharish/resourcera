# Disk and network

psutil provides visible filesystem capacity, aggregate disk read/write bytes and
operations per second, and per-interface traffic, packet, error, drop, and link-speed
observations where the platform exposes them. When OS timing counters are available,
`disk.latency_ms` estimates the host-wide mean milliseconds per completed read or
write over the sampling interval. This is not a per-device or tail-latency
measurement; timing precision and visible devices vary by platform. Latency is
unavailable during warmup, when no operations complete, or when timing counters
do not advance. Queue depth remains unsupported. The first rate sample is unavailable.

Measured throughput is **not** a capacity ceiling. To admit a workload requiring
disk or network throughput, configure the corresponding read, write, or combined
network budget in bytes per second. The estimator subtracts observed traffic,
logical claims, and the configured margin. Network accounting sums visible
interfaces and directions; loopback or overlapping interfaces may make it
conservative.

These are shared host observations, not per-process attribution or guaranteed
device-level bandwidth reservations. Process-level IPv4/IPv6 socket counts are
available as `snapshot.network_connections` when
`ResourceConfig(sample_process_connections=True)` is set. This expensive,
permission-dependent probe is disabled by default, sampled with disk/network
telemetry, and paused under a suspended overhead budget. Counts are estimated:
visibility can be incomplete, and they are not machine-wide connection tables.
Portable per-device queue depth and latency are not currently exposed.
