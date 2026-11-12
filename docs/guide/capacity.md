# Workloads and capacity

`WorkloadRequirements` accepts CPU equivalents, memory, disk read/write and network
bytes per second, accelerator type, **per-device** accelerator memory, accelerator
count, priority, safety margin, and an optional profile label. Built-in profile
names are `cpu-bound`, `memory-bound`, `io-bound`, `network-bound`, `gpu-bound`,
and `mixed`; custom labels are accepted. Profiles do not invent resource demands.

```python
from resourcera import WorkloadRequirements

requirements = WorkloadRequirements(cpu=0.5, memory="1GiB", profile="mixed")
result = await manager.capacity(requirements)
print(result.max_concurrency, result.limiting_resource, result.assumptions)
```

The estimate intersects fresh resource dimensions, applies pressure and margins,
subtracts all cooperating logical claims, and enforces the configured concurrency
ceiling. Unknown required data yields `max_concurrency=None`; a known exhausted
resource yields zero. Accelerator devices are evaluated individually.

Claims may overlap already-observed utilization; charging them in full is
deliberately conservative. Estimates do not guarantee future CPU, bandwidth,
memory, or accelerator compute, and device free memory is not free compute.
