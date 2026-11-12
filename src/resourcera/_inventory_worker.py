"""Collect slow inventory in a separately terminable process."""

from __future__ import annotations

import json
import platform
import sys
import time
from pathlib import Path

import psutil

from resourcera.accelerators import discover_platform


def collect() -> dict[str, object]:
    """Collect inventory without starting providers or modifying the environment."""
    items: dict[str, object] = {
        "platform": {"system": platform.system(), "machine": platform.machine()},
    }
    errors: list[str] = []
    accelerators = discover_platform()
    items["accelerators"] = accelerators.model_dump(mode="json")
    errors.extend(accelerators.diagnostics)
    try:
        nodes = (
            list(Path("/sys/devices/system/node").glob("node[0-9]*"))
            if sys.platform == "linux"
            else []
        )
        items["cpu"] = {
            "logical_cores": psutil.cpu_count(logical=True),
            "physical_cores": psutil.cpu_count(logical=False),
            "numa_nodes": len(nodes) if nodes else None,
        }
    except (OSError, psutil.Error) as exc:
        errors.append(f"CPU inventory: {exc}")
    try:
        partitions = psutil.disk_partitions(all=False)
        items["filesystems"] = [
            {"device": part.device, "mountpoint": part.mountpoint, "filesystem": part.fstype}
            for part in partitions
        ][:128]
        if len(partitions) > 128:
            errors.append("filesystem inventory was truncated at 128 entries")
    except (OSError, psutil.Error) as exc:
        errors.append(f"filesystem inventory: {exc}")
    try:
        interfaces = psutil.net_if_stats()
        items["interfaces"] = [
            {"name": name, "speed_mbps": info.speed if info.speed > 0 else None}
            for name, info in sorted(interfaces.items())[:256]
        ]
        if len(interfaces) > 256:
            errors.append("network inventory was truncated at 256 entries")
    except (OSError, psutil.Error) as exc:
        errors.append(f"network inventory: {exc}")
    return {"items": items, "complete": not errors, "diagnostics": errors}


def main() -> None:
    """Write a bounded inventory report to the supervising process."""
    report = collect()
    report["cpu_seconds"] = time.process_time()
    print(json.dumps(report, separators=(",", ":")))


if __name__ == "__main__":
    main()
