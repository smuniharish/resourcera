"""Combine local resource admission with one reused remote endpoint controller."""

import time

import anyio

from resourcera import (
    AdmissionRejectedError,
    RemoteConcurrencyController,
    RemoteConcurrencyRejectedError,
    ResourceManager,
)


async def fake_endpoint(index: int) -> int:
    await anyio.sleep(0.01)
    return 429 if index == 3 else 200


async def call(manager: ResourceManager, remote: RemoteConcurrencyController, index: int) -> str:
    try:
        # Local admission is acquired first and released by its context manager on every
        # path. The remote wait is bounded, so local capacity is not held indefinitely.
        async with (
            manager.admission({"memory": "64MiB"}, wait=True, wait_timeout_seconds=2),
            remote.slot(),
        ):
            started = time.monotonic()
            status = await fake_endpoint(index)
            if status in (429, 503):
                remote.report_overload(status_code=status)
            else:
                remote.report_success(latency_seconds=time.monotonic() - started)
        outcome = f"HTTP {status}"
    except AdmissionRejectedError as error:
        outcome = f"local admission rejected: {error.decision.reason}"
    except RemoteConcurrencyRejectedError as error:
        outcome = f"remote permit unavailable: {error}"
    return f"request {index}: {outcome}"


async def main() -> None:
    async with (
        ResourceManager() as manager,
        # Create the controller once per endpoint and reuse it for every request.
        manager.remote_concurrency(
            "inventory-api", initial_concurrency=2, max_concurrency=8, wait_timeout_seconds=5
        ) as remote,
    ):
        results: list[str] = []

        async def run(index: int) -> None:
            results.append(await call(manager, remote, index))

        async with anyio.create_task_group() as tasks:
            for index in range(8):
                tasks.start_soon(run, index)
        for line in sorted(results):
            print(line)
        state = remote.state
        print("Remote limit:", state.current_limit, "overloads:", state.overload_count)
        print("Local reservations held:", (await manager.admission_state()).active_reservations)


if __name__ == "__main__":
    anyio.run(main)
