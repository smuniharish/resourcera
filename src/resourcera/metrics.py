"""Helpers for constructing consistently timestamped metrics."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime

from resourcera.models import Metric, MetricStatus, Scope


@dataclass(frozen=True, slots=True)
class SampleClock:
    """Wall and monotonic timestamps captured together."""

    timestamp: datetime
    monotonic: float

    @classmethod
    def now(cls) -> SampleClock:
        """Capture a new pair of timestamps."""
        return cls(datetime.now(UTC), time.monotonic())


def metric[T](
    value: T | None,
    unit: str,
    source: str,
    scope: Scope,
    clock: SampleClock,
    fresh_for: float,
    *,
    status: MetricStatus | None = None,
    detail: str | None = None,
    confidence: float | None = None,
) -> Metric[T]:
    """Construct a metric without replacing missing values with zero."""
    resolved = status or (MetricStatus.AVAILABLE if value is not None else MetricStatus.UNAVAILABLE)
    return Metric[T](
        value=value,
        unit=unit,
        source=source,
        scope=scope,
        sampled_at=clock.timestamp,
        sampled_monotonic=clock.monotonic,
        fresh_for_seconds=fresh_for,
        measurement_status=resolved,
        detail=detail,
        confidence=confidence,
    )
