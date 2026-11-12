from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from resourcera.config import ResourceConfig
from resourcera.models import Metric, MetricStatus, Scope, WorkloadRequirements


def test_metric_distinguishes_zero_unknown_and_staleness() -> None:
    zero = Metric[int](
        value=0,
        unit="bytes",
        source="test",
        scope=Scope.PROCESS,
        sampled_at=datetime.now(UTC),
        sampled_monotonic=10,
        fresh_for_seconds=5,
        measurement_status=MetricStatus.AVAILABLE,
    )
    unknown = zero.model_copy(
        update={"value": None, "measurement_status": MetricStatus.UNAVAILABLE}
    )
    assert zero.status_at(15) is MetricStatus.AVAILABLE
    assert zero.status_at(15.01) is MetricStatus.STALE
    assert unknown.status_at(100) is MetricStatus.UNAVAILABLE
    assert unknown.value is None


@pytest.mark.parametrize(
    ("value", "status"),
    [(None, MetricStatus.AVAILABLE), (0, MetricStatus.UNAVAILABLE)],
)
def test_metric_rejects_contradictory_status(value: int | None, status: MetricStatus) -> None:
    with pytest.raises(ValidationError):
        Metric[int](
            value=value,
            unit="bytes",
            source="test",
            scope=Scope.PROCESS,
            sampled_at=datetime.now(UTC),
            sampled_monotonic=10,
            fresh_for_seconds=5,
            measurement_status=status,
        )


def test_workload_byte_units_and_invalid_demands() -> None:
    request = WorkloadRequirements.model_validate({"cpu": 2.0, "memory": "2GB"})
    assert request.cpu == 2.0
    assert request.memory == 2_000_000_000
    assert WorkloadRequirements.model_validate({"memory": "2GiB"}).memory == 2 * 1024**3
    for data in ({}, {"cpu": 0}, {"memory": "0B"}, {"cpu": float("inf")}):
        with pytest.raises(ValidationError):
            WorkloadRequirements.model_validate(data)


def test_config_rejects_unbounded_or_unknown_values() -> None:
    with pytest.raises(ValidationError):
        ResourceConfig(cpu_memory_interval_seconds=float("inf"))
    with pytest.raises(ValidationError):
        ResourceConfig.model_validate({"undeclared_setting": True})
