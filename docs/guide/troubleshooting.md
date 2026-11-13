# Troubleshooting

- **CPU capacity is unknown:** wait for two samples and check affinity, cgroup
  counters, sampling errors, and metric freshness.
- **Admission is rejected:** inspect `AdmissionRejectedError.decision`, including
  the limiting resource, reason, and per-resource statuses.
- **I/O capacity is unknown:** configure a real usable throughput ceiling; observed
  throughput alone does not establish one.
- **GPU detected without telemetry:** inspect provider status and installation
  guidance. Optional NVML and its driver must be installed separately.
- **Data is stale or admission is suspended:** inspect `snapshot.overhead.state`.
  The strict policy may pause expensive work until its budget recovers.
- **SQLite reports contention or a policy mismatch:** use the same local file and
  compatible settings for all participants. SQLite is not a network-filesystem or
  distributed coordination backend.
- **A lease was lost:** stop the work, investigate renewal or process pauses, and
  reacquire only after reviewing the new capacity decision.
- **A concurrency controller stays at its minimum:** check
  `controller.state.adjustment_reason`. Capacity for a required resource is
  unknown or stale, often because the manager has just started, or no network or
  disk ceiling is configured. Unknown capacity is never treated as unlimited.
- **A controller's limit is below its active count:** this is expected after
  capacity drops. Admitted work is never cancelled; new slots wait until work
  drains.
- **Slots are rejected with "waiter queue is full":** raise `max_waiters` or apply
  backpressure earlier. Waiting is deliberately bounded.

Provider detail and Python logging explain failures; do not substitute zeros for
missing values.
