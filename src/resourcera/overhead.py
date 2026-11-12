"""Bounded accounting and feedback for library-managed monitoring work."""

from __future__ import annotations

import math
import sys
from collections import deque
from dataclasses import dataclass
from enum import StrEnum

from pydantic import BaseModel

from resourcera.config import ResourceConfig


class OverheadState(StrEnum):
    """Current overhead-governance level."""

    NORMAL = "normal"
    REDUCED = "reduced"
    SUSPENDED = "suspended"


@dataclass(frozen=True, slots=True)
class ManagedSize:
    """A bounded estimate of selected Python-owned object graphs."""

    bytes: int
    complete: bool


def managed_size(*objects: object, max_objects: int = 100_000) -> ManagedSize:
    """Estimate reachable Python object size without traversing arbitrary application objects."""
    pending = list(objects)
    seen: set[int] = set()
    total = 0
    while pending:
        if len(seen) >= max_objects:
            return ManagedSize(total, False)
        obj = pending.pop()
        identity = id(obj)
        if identity in seen:
            continue
        seen.add(identity)
        total += sys.getsizeof(obj)
        if isinstance(obj, BaseModel):
            pending.append(obj.__dict__)
        elif isinstance(obj, dict):
            pending.extend(obj.keys())
            pending.extend(obj.values())
        elif isinstance(obj, (tuple, list, set, frozenset, deque)):
            pending.extend(obj)
    return ManagedSize(total, True)


class OverheadController:
    """Apply a sliding CPU budget and hysteresis to optional monitoring work."""

    def __init__(self, config: ResourceConfig) -> None:
        self._config = config
        self._bucket_width = config.cpu_budget_window_seconds / 60
        self._buckets: deque[tuple[int, float, float]] = deque()
        self._cpu_seconds = 0.0
        self._bootstrap_seconds = 0.0
        self._bootstrap_used = 0.0
        self._bootstrap_allowance = min(
            config.cpu_budget_fraction * config.cpu_budget_window_seconds, 1.0
        )
        self._last_time: float | None = None
        self._managed_bytes = 0
        self._size_complete = True
        self._state = OverheadState.NORMAL
        self.last_provider_cpu: float | None = None
        self.last_provider_duration: float | None = None
        self.last_accelerator_cpu: float | None = None
        self.unmeasured_provider_work = False

    @property
    def state(self) -> OverheadState:
        """Return the last evaluated feedback state."""
        return self._state

    @property
    def managed_bytes(self) -> int:
        """Return the latest estimated retained-state size."""
        return self._managed_bytes

    @property
    def size_complete(self) -> bool:
        """Indicate whether the size traversal reached all selected objects."""
        return self._size_complete

    @property
    def retained_objects(self) -> tuple[object, ...]:
        """Return bounded accounting buckets for managed-state estimates."""
        return (self._buckets,)

    def _expire(self, now: float) -> None:
        if not math.isfinite(now) or now < 0:
            raise ValueError("monotonic time must be finite and nonnegative")
        if self._last_time is not None and now < self._last_time:
            raise ValueError("overhead observations must use a monotonic clock")
        self._last_time = now
        earliest = math.floor((now - self._config.cpu_budget_window_seconds) / self._bucket_width)
        while self._buckets and self._buckets[0][0] < earliest:
            _, cpu, bootstrap = self._buckets.popleft()
            self._cpu_seconds -= cpu
            self._bootstrap_seconds -= bootstrap
        self._cpu_seconds = max(0.0, self._cpu_seconds)
        self._bootstrap_seconds = max(0.0, self._bootstrap_seconds)

    def record_cpu(self, seconds: float, now: float, *, bootstrap: bool = False) -> None:
        """Charge attributable CPU time to the current fixed-size time bucket."""
        if not math.isfinite(seconds) or seconds < 0:
            raise ValueError("CPU cost must be finite and nonnegative")
        self._expire(now)
        bucket = math.floor(now / self._bucket_width)
        allowance = (
            min(seconds, max(0.0, self._bootstrap_allowance - self._bootstrap_used))
            if bootstrap
            else 0.0
        )
        self._bootstrap_used += allowance
        if self._buckets and self._buckets[-1][0] == bucket:
            key, previous, previous_bootstrap = self._buckets.pop()
            self._buckets.append((key, previous + seconds, previous_bootstrap + allowance))
        else:
            self._buckets.append((bucket, seconds, allowance))
        self._cpu_seconds += seconds
        self._bootstrap_seconds += allowance
        self.evaluate(now)

    def record_provider(
        self,
        cpu_seconds: float | None,
        duration: float,
        now: float,
        *,
        accelerator: bool,
        bootstrap: bool = False,
    ) -> None:
        """Record completed or unattributable provider work."""
        if not math.isfinite(duration) or duration < 0:
            raise ValueError("provider duration must be finite and nonnegative")
        self.last_provider_cpu = cpu_seconds
        self.last_provider_duration = duration
        if accelerator:
            self.last_accelerator_cpu = cpu_seconds
        if cpu_seconds is None:
            self.unmeasured_provider_work = True
            self.evaluate(now)
        else:
            self.record_cpu(cpu_seconds, now, bootstrap=bootstrap)

    def set_managed_size(self, size: ManagedSize, now: float) -> None:
        """Update retained-state accounting and immediately evaluate its budget."""
        self._managed_bytes = size.bytes
        self._size_complete = size.complete
        self.evaluate(now)

    def cpu_seconds(self, now: float) -> float:
        """Return measured CPU seconds in the conservatively bucketed window."""
        self._expire(now)
        return self._cpu_seconds

    def bootstrap_seconds(self, now: float) -> float:
        """Return the one-time startup allowance currently represented in the window."""
        self._expire(now)
        return self._bootstrap_seconds

    def evaluate(self, now: float) -> OverheadState:
        """Apply 80%/100% escalation and 70%/60% recovery hysteresis."""
        self._expire(now)
        cpu_budget = self._config.cpu_budget_fraction * self._config.cpu_budget_window_seconds
        cpu_ratio = (self._cpu_seconds - self._bootstrap_seconds) / cpu_budget
        memory_ratio = self._managed_bytes / self._config.managed_state_budget_bytes
        ratio = max(cpu_ratio, memory_ratio, 0.0 if self._size_complete else 1.0)
        if ratio >= 1 or (self._state is OverheadState.SUSPENDED and ratio >= 0.7):
            self._state = OverheadState.SUSPENDED
        elif ratio >= 0.8 or (self._state is not OverheadState.NORMAL and ratio >= 0.6):
            self._state = OverheadState.REDUCED
        else:
            self._state = OverheadState.NORMAL
        return self._state

    def interval(self) -> float:
        """Return the governed CPU/memory interval, retaining a recovery path."""
        base = self._config.cpu_memory_interval_seconds
        if self._state is OverheadState.SUSPENDED:
            return max(base * 4, min(self._config.cpu_budget_window_seconds / 4, 30))
        if self._state is OverheadState.REDUCED:
            return base * 2
        return base
