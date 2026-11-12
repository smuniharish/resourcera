# Changelog

## Unreleased

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
