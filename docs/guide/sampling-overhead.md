# Hybrid sampling and strict overhead

CPU and memory are periodically sampled; disk/network rates use a slower interval;
inventory is refreshed through refresh-engine. Linux link-change events can trigger
inventory refresh when the netlink interface is available; other situations use
polling. Accelerator detail can also be requested on demand.

The overhead controller measures attributable sampler, control, coordination, and
completed provider CPU time over a rolling window, plus estimated retained managed
objects. At 80% of a budget it reduces optional work; at 100% it suspends optional
probes and new admission, keeps a recovery path, and marks affected accelerator
data stale. Recovery uses hysteresis. Sample intervals increase under pressure rather
than blindly polling faster.

The default is 0.6 CPU-seconds per 60-second window after a one-time allowance of at
most 0.6 CPU-seconds, and 16 MiB estimated managed state. A single already-running
probe can cross a threshold before feedback is applied. These targets do not
preempt arbitrary native code, bound total application CPU, or measure opaque native
allocations perfectly. Failed/terminated child CPU and library-attributable disk and
network bytes can be unavailable. Shared worker threads are not exclusively owned
by the library. The owned-task metric counts directly managed runtime tasks at
sampling time, excluding refresh-engine internals and application-owned calls; the
owned-thread metric counts dedicated bridge threads, not shared workers.

An uninterruptible system call can outlive a timeout. Its worker remains bounded
and is not replaced indefinitely. Vendor probes are isolated in terminable child
processes, subject to OS process-termination behavior.
