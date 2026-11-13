---
name: resourcera-provider
description: >-
  Use this skill when implementing, testing, or debugging a custom RESOURCERA
  accelerator provider. Do not use it for ordinary snapshots, admission, or
  reservation integration; use resourcera instead.
license: Apache-2.0
metadata:
  author: S MUNI HARISH
  version: "0.2.0"
---

# Custom accelerator providers

RESOURCERA runs custom provider callables in isolated, terminable child processes.
Extend the existing provider contract rather than replacing discovery or inventing
a vendor SDK.

## Workflow

1. Inspect the target platform's existing interfaces and the installed RESOURCERA
   version. Reuse an existing OS or vendor interface; do not download packages,
   alter drivers, or import heavyweight ML frameworks for discovery.
2. Implement an importable `module:callable` returning a `ProviderReport` (or an
   awaitable producing one). Provide `ProviderInfo` and bounded `DeviceReading`
   records. The module must also be importable by the child process.
3. Declare supported OSes, vendors, device types, metrics, and limitations. Use
   stable IDs and UUIDs when available, explicit device and metric statuses, and
   only observed values.
4. Configure the callable in `ResourceConfig.accelerator_providers`. Keep provider
   work bounded and allow timeout or failure to be reported independently.
5. Test absent interfaces and devices, partial telemetry, errors, timeouts, import
   failures, and multiple devices with deterministic fakes. Also exercise the
   configured provider through `ResourceManager`.
6. Run focused tests and check the actual supported platform where possible. If
   real hardware was not available, report that limitation.

## Rules

- Visibility is not proof of accessibility; utilization is not allocatable compute.
- Unknown, unsupported, or failed telemetry must not be reported as zero.
- Do not pool device memory or assign the same physical device multiple identities
  when a shared UUID is available.
- Do not require hardware, driver installation, network access, or privileged
  changes for ordinary tests.

## References

- [Provider contract and statuses](references/provider-contract.md)
- [Validation and failure cases](references/validation.md)
