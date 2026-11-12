"""Admit a bounded workload or explain rejection. Requires: pip install resourcera."""

import anyio

from resourcera import AdmissionRejectedError, ResourceManager


async def main() -> None:
    async with ResourceManager() as resources:
        try:
            async with resources.admission({"memory": "32MiB"}, wait=True) as decision:
                print("Accepted:", decision.lease_id, decision.reason)
                await anyio.sleep(0.01)
        except AdmissionRejectedError as error:
            print("Rejected:", error.decision.reason)


if __name__ == "__main__":
    anyio.run(main)
