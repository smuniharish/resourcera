# CPU, memory, and containers

CPU information comes from psutil and applicable OS interfaces: logical and
physical counts, per-core and aggregate utilization, affinity, frequency, process
CPU and threads, and load average where supported. Linux also exposes the number
of runnable kernel scheduling entities, including processes and threads. CPU
capacity is measured in estimated **CPU equivalents**, never as a fixed number
of free threads.

On Linux, cgroup v1/v2 quota and memory hierarchies are read from the current
process's mount namespace. CPU headroom intersects affinity and idle CPU with
every finite ancestor quota and its observed usage, including siblings. Memory
headroom is the minimum of host available memory and every finite cgroup ancestor's
remaining memory. Unreadable limits or counters remain unknown.

Memory metrics distinguish host available/used memory, process RSS, psutil's VMS,
USS, swap, and cgroup usage. VMS is virtual address size on Unix-like systems but
Windows `PagefileUsage` in psutil; it is not resident physical memory or a
portable total-address-space measure. Host availability already reflects existing
consumption; process RSS is not subtracted again. RSS, USS, host memory, cgroup
memory, and managed library state overlap and must not be added as independent
allocations. Container identity is best-effort and namespace-dependent.

Linux pressure stall information is included when available. NUMA node counts are
used in the slow internal inventory; detailed NUMA placement is not currently
exposed as a public metric.
