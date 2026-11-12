# Provider contract and statuses

Set `ResourceConfig(accelerator_providers=("package.module:callable",))`.
The callable is invoked in a child process and returns a `ProviderReport`, or an
awaitable resolving to one. The installed module and any required dependencies
must be importable in that child.

`ProviderReport` contains `provider: ProviderInfo`, a bounded tuple of
`DeviceReading` records, and a `complete` flag. Describe supported platforms,
vendors, device types, metrics, dependencies, and limitations in `ProviderInfo`.
Each device needs a stable `id`, `kind`, `source`, and device statuses. Supply a
UUID when the underlying interface exposes one.

Report `DEVICE_NOT_DETECTED`, `DEVICE_UNSUPPORTED`, `DEVICE_INACCESSIBLE`,
`PARTIAL_TELEMETRY`, or `PROVIDER_ERROR` as appropriate; a detected device is
not necessarily accessible. Use `metric_status` and `metric_detail` to distinguish
unavailable, unsupported, and failed metrics. Missing readings remain `None`.
Do not fabricate free memory or interpret utilization as free compute.

The repository's `examples/custom_provider.py` demonstrates Linux accelerator
class discovery, including unsupported and error reports without fake telemetry.
