# Changelog

## Unreleased (0.3.0)

- Refresh runtime, optional, development, documentation, build, and transitive
  dependencies to current compatible stable releases.
- Distinguish omitted admission timeouts (configured default) from an explicit
  `None` (indefinite wait). This intentionally changes the meaning of explicitly
  passing `None`; omit the argument for the previous configured timeout.
- Configure controller-permit and resource-admission timeouts independently and
  wake waiters at either stage when their controller or manager closes.
- Expose manager-local admission queue state and distinguish controller permit
  waiters from operations awaiting a lease.
- Add separate process-local remote concurrency controllers with feedback,
  cooldown, Retry-After, remote queue-wait observations and application-reported
  capacity ceilings.
- Delay initial local calibration until measurements are fresh and monitoring is
  not degraded.
- Defer costly Windows swap sampling to the slower sampling cycle, preserving
  unavailable and stale status rather than inventing measurements.
- Never return a lease once manager shutdown has begun: a reservation that
  commits while `close()` is running is released and the caller receives
  `ManagerStateError`. Previously, a reservation that committed concurrently with
  `close()` could be returned after shutdown had already released it.
- Bind remote controllers and adaptive-controller feedback to their event-loop
  thread. Feedback or acquisition from another thread now raises `RuntimeError`
  instead of mutating AnyIO limiter state off-loop; use
  `anyio.from_thread.run_sync` from worker threads.
- Document remote adaptation windows, low-traffic and no-feedback behavior,
  failure classification, controller reuse, local and remote admission
  composition, shutdown semantics, admission fairness, and thread cancellation;
  add the `local_remote_composition.py` example.
- Judge remote adaptation on a bounded window of fresh outcomes: at most
  `max_samples` (default 256) no older than `sample_max_age_seconds` (default the
  larger of 30 seconds and four observation windows), using the window's own mean
  latency. Previously, arbitrarily old outcomes and smoothed latency history
  counted as current evidence; sparse endpoints now hold instead of adapting on
  stale feedback.
- Prevent low-priority admission starvation with bounded bypass: equal priorities
  stay FIFO, and at most `ResourceConfig.admission_max_bypass` later
  higher-priority requests can be inserted ahead of a waiter (default 8; 0 gives
  arrival order). The budget is charged at insertion, so cancelled or timed-out
  overtakers still count. Previously, strict priority ordering could starve lower
  priorities indefinitely.
- Apply the event-loop ownership contract to every controller operation: remote
  and adaptive `state` (and `current_limit`), `adjust()`, context entry and exit,
  and slot release now reject foreign threads with `RuntimeError` instead of
  touching AnyIO limiters or cancel scopes off-loop. Reads remain allowed before
  a controller is bound.
- Make lazy owner binding atomic so concurrent first use from several threads
  binds exactly one owner.
- Bind controllers to their event loop, not just its thread: a later event loop
  on the same thread, a call made outside any loop, or any use after the owning
  loop has ended now raises `RuntimeError` instead of being accepted because the
  thread matched. Ownership never rebinds, and closing a manager after a
  controller's loop ended completes without error.
- Closing a manager from a non-owner thread now marks its controllers closed
  immediately and cancels their waiters on the owning event loop instead of
  cancelling AnyIO scopes from the closing thread.
- Validate concurrency counts strictly: concurrency bounds and `max_waiters`
  reject booleans, floats and strings with `TypeError`, and `ResourceConfig`
  count fields are strict integers. Numeric strings such as `"8"` and floats such
  as `2.0` were previously coerced and are now rejected.

## Unreleased (0.2.1)

- Add live PyPI download badges to the README and documentation homepage.
- Simplify selected architecture diagrams and explain the adaptive concurrency
  policy at a higher level while retaining user-facing guarantees.
- Document verified dependency-provided agent skills and correct PyPI installation
  and release information.
- Refresh the dependency lockfile and development tooling, and clarify the
  maintainer-controlled release process.

## 0.2.0

- Add `ResourceManager.recommend_concurrency`, which turns capacity estimates into
  bounded, explainable concurrency and worker-count recommendations with confidence,
  freshness, and assumptions.
- Add `ResourceManager.adaptive_concurrency` and `ConcurrencyController`: leased slots
  with responsive decreases, cautious increases, application overload feedback,
  bounded FIFO waiters, and cancellation-safe shutdown.
- Add `ConcurrencyController.run_sync` for bounded submissions to an application-owned
  executor. A permit is released only after the submitted callable finishes.
- Add the `ConcurrencyRecommendation`, `ConcurrencyState`, and `ConcurrencyAdjustment`
  models, the `disk-bound` workload profile, and configuration for the adjustment
  interval, increase step, and history size.
- Scale configured disk-throughput capacity by reported I/O pressure.
- Add adaptive-concurrency documentation and examples for thread pools, asyncio, AnyIO,
  network-bound work, disk-bound work, multiple controllers, and controller state.
- Treat the real cgroup v2 root, which has no `cpu.max` or `memory.max`, as unlimited
  rather than unknown, so hosts that run processes in or under it report CPU and memory
  capacity.

## 0.1.0

- Handle unsupported CPU frequency on macOS and fix the hosted documentation build.
- Add RESOURCERA branding, a redesigned documentation homepage, and expanded agent skills.
- Use a pinned global Mermaid renderer for diagram regeneration, without Node project files.
- Add async resource snapshots, psutil telemetry including process virtual memory,
  Linux runnable-task observations, optional process socket counts, and
  cgroup-aware limits.
- Add automatic OS-visible accelerator inventory and optional isolated NVIDIA NVML
  telemetry, with explicit missing and partial capability states.
- Add bounded hybrid sampling, attributable overhead feedback, and refresh-engine
  inventory reconciliation.
- Add explainable capacity estimates, bounded resource-aware admission, process-local
  leases, and opt-in same-machine SQLite coordination.
- Add asyncio and Trio support, documentation, examples, diagrams, and development checks.
