# Examples

The repository's `examples/` directory contains standalone, hardware-optional
programs. Run them from the repository root after installing the project:

```shell
uv run python examples/basic_snapshot.py
uv run python examples/resource_admission.py
```

| Example | Demonstrates |
| --- | --- |
| `basic_snapshot.py` | Cached snapshots and metric status |
| `process_resources.py`, `machine_resources.py` | Visibility scopes |
| `cpu_monitoring.py`, `memory_monitoring.py` | CPU and memory |
| `disk_monitoring.py`, `network_monitoring.py` | Filesystems and rates |
| `automatic_accelerator_discovery.py` | Hardware-optional discovery |
| `hybrid_sampling.py`, `strict_overhead.py` | Sampling and overhead feedback |
| `capacity_estimation.py`, `workload_requirements.py` | Estimates and explicit profiles |
| `resource_admission.py`, `adaptive_concurrency.py` | Admission and bounded work |
| `threadpool_workers.py` | Sizing a `ThreadPoolExecutor` and bounding its submissions |
| `adaptive_asyncio.py`, `adaptive_anyio.py` | Replacing fixed semaphores and capacity limiters |
| `network_concurrency.py` | Network-bound limits and overload feedback |
| `disk_concurrency.py` | Disk-bound file processing |
| `multiple_controllers.py` | Independent controllers sharing reservations |
| `concurrency_state.py` | Controller state and adjustment history |
| `resource_reservations.py` | Logical lease lifecycle |
| `container_resources.py` | Cgroup limits when available |
| `custom_provider.py` | An isolated, hardware-optional custom provider |

Each closes its manager and handles unavailable hardware or rejected admission.
The custom provider example must be run from the repository root so its module is
importable in the child process. The NVIDIA extra is optional for all examples.
