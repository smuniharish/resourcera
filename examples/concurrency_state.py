"""Inspect a controller's state and adjustment history. Requires: pip install resourcera."""

import anyio

from resourcera import ResourceManager


async def main() -> None:
    async with (
        ResourceManager(scope="machine") as manager,
        manager.adaptive_concurrency({"memory": "32MiB"}, max_concurrency=10) as controller,
    ):
        async with controller.slot():
            state = controller.state
            print("Current limit:", state.current_limit)
            print("Recommended limit:", state.recommended_limit)
            print("Bounds:", state.min_concurrency, state.max_concurrency)
            print("Active:", state.active_count, "pending:", state.pending_count)
            print("Available permits:", state.available_permits)
            print("Limiting resource:", state.limiting_resource)
            print("Confidence:", state.confidence)
            print("Reason:", state.adjustment_reason)

        controller.signal_overload("A downstream timeout was observed.")
        state = await controller.adjust()
        for adjustment in state.history:
            print(
                "Adjustment:",
                adjustment.previous_limit,
                "->",
                adjustment.new_limit,
                adjustment.reason,
            )


if __name__ == "__main__":
    anyio.run(main)
