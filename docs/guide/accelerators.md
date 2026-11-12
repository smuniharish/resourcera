# Automatic accelerator discovery

No vendor needs to be selected by the application. Discovery examines existing
OS-visible interfaces and lazily activates compatible installed providers:

| Platform/interface | Current capability |
| --- | --- |
| Linux PCI | Display and accelerator-class identities; known Coral Edge TPU and Intel NPU driver identities. |
| Linux AMD `amdgpu` sysfs | VRAM and utilization when the driver exposes them. |
| Windows ConfigMgr | Present display interfaces and identifiable vendors; not proof of compute access. |
| macOS IOKit | Visible `IOAccelerator` services; detailed telemetry is not supplied. |
| NVIDIA NVML | Optional `nvidia` extra; memory, utilization, identity, driver, and limited process data where supported. |

Other TPU, NPU, Intel, Apple, or AMD management APIs are not universally available
and are not claimed as supported telemetry. An isolated custom provider may add
capabilities. Detection, provider installation, access, unsupported telemetry,
partial telemetry, and provider errors are reported separately.

No SDKs, drivers, or frameworks are installed automatically. One failing provider
does not disable independent providers. Device visibility can differ from host
visibility, and utilization is not allocatable compute capacity. Accelerator memory
requirements apply **per device**; memory from different devices is not pooled.
Known UUIDs prevent duplicate aliases from being reserved twice. Providers must
supply stable identities; aliases without a shared UUID cannot always be correlated.
