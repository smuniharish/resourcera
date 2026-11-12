# User guide

Learn how RESOURCERA turns local observations into conservative capacity and
admission decisions.

<div class="grid cards" markdown>

-   :material-chart-line: **Understand observations**

    [Architecture and scopes](architecture.md) · [Metrics and freshness](metrics.md) ·
    [CPU, memory, and containers](cpu-memory.md) · [Disk and network](disk-network.md)

-   :material-memory: **Discover accelerators**

    [Accelerator capabilities](accelerators.md) · [Custom providers](custom-providers.md)

-   :material-scale-balance: **Control work**

    [Workloads and capacity](capacity.md) · [Admission](admission.md) ·
    [Reservations and coordination](coordination.md)

-   :material-heart-pulse: **Operate safely**

    [Sampling and overhead](sampling-overhead.md) · [Troubleshooting](troubleshooting.md)

</div>

Resource estimates are not guarantees, and logical leases are not physical
isolation. Review [coordination](coordination.md) before sharing reservations
between processes.
