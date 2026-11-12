# The resourcera-provider skill

The `resourcera-provider` skill helps a coding agent extend accelerator discovery
using an importable callable and the existing isolated provider contract.

[Install this skill](install.md){ .md-button .md-button--primary }

## What it guides

1. Reuse an existing OS or installed vendor interface.
2. Return bounded `ProviderReport` and `DeviceReading` records with stable identities.
3. Declare platform support, capabilities, limitations, and explicit error states.
4. Test absent devices, partial telemetry, timeouts, import failures, and multiple
   devices without requiring accelerator hardware.
5. Exercise the configured provider through `ResourceManager` and state whether
   real hardware was tested.

The skill does not install SDKs or drivers, equate device visibility with access,
or turn unknown metrics into zero.

## What's inside

```text
resourcera-provider/
├── SKILL.md
├── references/
│   ├── provider-contract.md
│   └── validation.md
└── evals/
    ├── evals.json
    └── trigger_queries.json
```

See the [custom provider guide](../guide/custom-providers.md) for the application
configuration and the [accelerator guide](../guide/accelerators.md) for current
platform limitations.
