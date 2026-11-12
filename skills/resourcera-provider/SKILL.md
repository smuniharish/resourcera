---
name: resourcera-provider
description: Implement and validate an isolated RESOURCERA accelerator provider.
---

# Accelerator providers

Configure an importable `module:callable` in `ResourceConfig.accelerator_providers`.
The callable is invoked in a bounded child process and returns `ProviderReport`
with `ProviderInfo` and zero or more `DeviceReading` records. The module must be
importable by that child, not only in the parent interpreter.

- Use already-installed OS or vendor interfaces; never install packages, download
  runtimes, alter drivers, or import heavyweight ML frameworks merely for discovery.
- Report stable device IDs and UUIDs where available. Do not equate device
  visibility with accessibility or utilization with allocatable compute.
- Supply actual measurements and explicit unsupported, unavailable, partial, or
  error states. Do not fill unknown measurements with zero.
- Bound enumeration and response size. Declare supported OSes, vendors, device
  types, metrics, and limitations; preserve independent providers on failure.
- Test missing interfaces, absent devices, partial telemetry, timeouts, import
  errors, and multiple devices without requiring physical hardware.

See `examples/custom_provider.py`, `docs/guide/custom-providers.md`, and
`tests/test_accelerators.py` in the repository.
