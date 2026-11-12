"""Discover visible accelerators. Optional: pip install 'resourcera[nvidia]'."""

import anyio

from resourcera import ResourceManager


async def main() -> None:
    async with ResourceManager() as resources:
        snapshot = await resources.snapshot()
        if not snapshot.accelerators:
            print("No accelerator was visible to the available discovery interfaces.")
        for device in snapshot.accelerators:
            print(device.id, device.vendor, device.kind, [state.value for state in device.statuses])
            print(
                "Device memory:",
                device.memory_total_bytes.value,
                device.memory_total_bytes.unit,
                device.memory_total_bytes.status.value,
            )
        for provider in snapshot.providers:
            print("Provider:", provider.name, [state.value for state in provider.statuses])
            if provider.detail:
                print(" ", provider.detail)


if __name__ == "__main__":
    anyio.run(main)
