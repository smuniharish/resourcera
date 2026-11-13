# Changelog

## Unreleased (0.2.0)

- Add `ResourceManager.recommend_concurrency`, which turns capacity estimates into
  bounded, explainable concurrency and worker-count recommendations with confidence,
  freshness, and assumptions.
- Add `ResourceManager.adaptive_concurrency` and `ConcurrencyController`: leased slots
  on an AnyIO capacity limiter with a deterministic AIMD policy (immediate decreases,
  smoothed and rate-limited increases, calibration once capacity becomes measurable,
  and application overload feedback), bounded FIFO waiters, and cancellation-safe
  shutdown.
- Add `ConcurrencyController.run_sync` for bounded submissions to an application-owned
  executor. A permit is released only after the submitted callable finishes.
- Add the `ConcurrencyRecommendation`, `ConcurrencyState`, and `ConcurrencyAdjustment`
  models, the `disk-bound` workload profile, and configuration for the adjustment
  interval, increase step, and history size.
- Scale configured disk-throughput capacity by reported I/O pressure.
- Add adaptive-concurrency documentation and examples for thread pools, asyncio, AnyIO,
  network-bound work, disk-bound work, multiple controllers, and controller state.

## 0.1.0 (unpublished)

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
