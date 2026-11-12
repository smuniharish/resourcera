# The resourcera skill

The `resourcera` skill helps a coding agent integrate or troubleshoot the public
API. It covers the manager lifecycle, metric freshness, workload requirements,
capacity estimates, admission, and logical reservations.

[Install this skill](install.md){ .md-button .md-button--primary }

## What it guides

1. Inspect the application and its actual resource requirements.
2. Verify the local Python setup and open a manager for the work's lifetime.
3. Keep unknown measurements distinct from zero or unlimited capacity.
4. Hold admission while work runs and explicitly handle rejection or lease loss.
5. Test cancellation, cleanup, and the behavior under resource pressure.

For implementing a custom accelerator provider, use
[resourcera-provider](resourcera-provider.md) instead.

## What's inside

```text
resourcera/
├── SKILL.md
├── references/
│   ├── runtime.md
│   └── workloads.md
├── scripts/
│   └── verify_setup.py
└── evals/
    ├── evals.json
    └── trigger_queries.json
```

The setup check runs offline, verifies Python 3.12+ and package imports, then
opens a manager, obtains a snapshot, and closes it. It reports metric status and
visible accelerators without requiring every metric or device to be available.
`--json` emits a single report; a failure exits with status 1.
