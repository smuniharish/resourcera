"""Read host and process memory separately. Requires: pip install resourcera."""

import anyio

from resourcera import ResourceManager


async def main() -> None:
    async with ResourceManager() as resources:
        memory = (await resources.snapshot()).memory
        for label, reading in (
            ("Host total", memory.total_bytes),
            ("Host available", memory.available_bytes),
            ("Process RSS", memory.process_rss_bytes),
            ("Process VMS", memory.process_vms_bytes),
            ("Process USS", memory.process_uss_bytes),
            ("Cgroup available", memory.cgroup_available_bytes),
        ):
            print(label, reading.value, reading.unit, reading.status.value)
        print("Cgroup limit state:", memory.cgroup_limit_state.value)


if __name__ == "__main__":
    anyio.run(main)
