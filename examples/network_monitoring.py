"""Inspect interface rates. Requires: pip install resourcera."""

import anyio

from resourcera import ResourceConfig, ResourceManager


async def main() -> None:
    config = ResourceConfig(
        cpu_memory_interval_seconds=0.25,
        disk_network_interval_seconds=0.25,
        sample_process_connections=True,
    )
    async with ResourceManager(config) as resources:
        await anyio.sleep(0.4)
        snapshot = await resources.snapshot()
        interfaces = snapshot.network
        if not interfaces:
            print("No network interfaces were visible.")
        for interface in interfaces:
            print(
                interface.name,
                "receive:",
                interface.received_bytes_per_second.value,
                interface.received_bytes_per_second.unit,
                interface.received_bytes_per_second.status.value,
            )
        print(
            "Visible process Internet connections:",
            snapshot.network_connections.value,
            snapshot.network_connections.status.value,
        )


if __name__ == "__main__":
    anyio.run(main)
