"""Check RESOURCERA's Python environment and snapshot lifecycle offline."""

from __future__ import annotations

import argparse
import json
import platform
import sys
from typing import TypedDict


class Check(TypedDict):
    """One setup-check result."""

    status: str
    check: str
    detail: str


def main() -> int:
    """Report a bounded, read-only snapshot smoke test."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Output one JSON report")
    args = parser.parse_args()
    checks: list[Check] = []

    def report(status: str, check: str, detail: str) -> None:
        checks.append({"status": status, "check": check, "detail": detail})

    def finish() -> int:
        ok = all(check["status"] != "FAIL" for check in checks)
        if args.json:
            print(json.dumps({"ok": ok, "checks": checks}))
        else:
            for check in checks:
                print(f"{check['status']}  {check['check']}: {check['detail']}")
            print("All checks passed." if ok else "Setup check failed.")
        return 0 if ok else 1

    compatible = sys.version_info >= (3, 12)
    report(
        "PASS" if compatible else "FAIL",
        "python",
        f"{platform.python_version()} (3.12 or newer required)",
    )
    if not compatible:
        return finish()

    try:
        import anyio

        import resourcera
        from resourcera import ResourceError, ResourceManager
    except (ImportError, OSError) as error:
        report("FAIL", "imports", f"{error}. Install RESOURCERA in this Python environment.")
        return finish()

    report("PASS", "imports", f"RESOURCERA {resourcera.__version__} and required imports")

    async def snapshot_check() -> tuple[str, str, int]:
        with anyio.fail_after(15):
            async with ResourceManager() as resources:
                snapshot = await resources.snapshot()
                return (
                    snapshot.memory.available_bytes.status.value,
                    snapshot.overhead.state,
                    len(snapshot.accelerators),
                )

    try:
        memory_status, overhead_state, device_count = anyio.run(snapshot_check)
    except (ResourceError, OSError, TimeoutError) as error:
        report("FAIL", "snapshot", f"{type(error).__name__}: {error}")
    else:
        report("PASS", "snapshot", "Manager opened, returned a snapshot, and closed")
        report(
            "INFO",
            "capabilities",
            f"memory status={memory_status}; overhead={overhead_state}; "
            f"visible accelerators={device_count}",
        )
    return finish()


if __name__ == "__main__":
    raise SystemExit(main())
