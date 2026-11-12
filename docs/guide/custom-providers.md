# Custom accelerator providers

Set `accelerator_providers` to importable `module:callable` paths. The callable
returns a `ProviderReport` (or an awaitable producing one) containing a
`ProviderInfo` and bounded `DeviceReading` records. It runs in a separate,
terminable Python process, so the module must be importable there.

Use stable IDs and UUIDs, accurate device statuses, and explicit metric statuses.
Do not install packages, change drivers, or report unknown data as zero. A provider
must document its OS, vendor, visibility, supported metrics, and limitations.
Provider errors and timeouts are reported independently; other providers continue.

The executable [custom provider example](../examples/index.md) inspects the Linux
accelerator class when available and reports no detailed telemetry.
