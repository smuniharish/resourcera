# Architecture and scopes

The manager combines lightweight psutil sampling with slower refresh-engine
inventory reconciliation. Blocking system calls use bounded worker threads;
hardware providers run in terminable subprocesses. Asyncio is the runtime backend;
Trio callers use one managed asyncio bridge.

- **Process scope** identifies the current process and its measurable consumption.
- **Machine scope** also reports host observations visible to the process.
- **Distributed scope** is explicitly unsupported; no cluster backend is supplied.

Host, process, and cgroup measurements retain their own source and scope. Process
scope does not make host disk/network counters per-process. Local snapshots may be
limited by containers, permissions, and device visibility.

The default inventory state store is in-memory. A caller may provide a
refresh-engine `StateStore` through `ResourceManager(inventory_store=...)` for a
compatible durable implementation; it must work on an asyncio event loop, including
when the caller uses Trio. RESOURCERA does not ship a durable inventory store.
Reservations use a separate, optional SQLite store.

See the [diagrams](../architecture/diagrams.md).
