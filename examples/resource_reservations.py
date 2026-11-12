"""Acquire and release an interpreter-local lease. Requires: pip install resourcera."""

import anyio

from resourcera import AdmissionRejectedError, ReservationError, ResourceManager


async def main() -> None:
    async with ResourceManager() as resources:
        try:
            lease = await resources.reserve({"memory": "32MiB"})
        except AdmissionRejectedError as error:
            print("Reservation rejected:", error.decision.reason)
            return
        try:
            print("Lease:", lease.id, "expires:", lease.expires_at)
            lease = await resources.renew(lease)
            print("Renewed until:", lease.expires_at)
            print("Current lease:", (await resources.reservation(lease)).id)
        except ReservationError as error:
            print("Lease lost; stop the workload:", error)
        finally:
            try:
                await resources.release(lease)
            except ReservationError as error:
                print("Lease could not be released:", error)


if __name__ == "__main__":
    anyio.run(main)
