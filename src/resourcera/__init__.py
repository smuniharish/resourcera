"""Resource intelligence and adaptive capacity for Python applications."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from resourcera.accelerators import DeviceReading, ProviderReport
    from resourcera.concurrency import ConcurrencyController, ConcurrencySlot
    from resourcera.config import ResourceConfig
    from resourcera.exceptions import (
        AdmissionRejectedError,
        ManagerStateError,
        OverheadBudgetError,
        ProviderError,
        ReservationError,
        ResourceError,
        UnsupportedScopeError,
    )
    from resourcera.manager import ResourceManager
    from resourcera.models import (
        AcceleratorDevice,
        AdmissionDecision,
        CapacityResult,
        ConcurrencyAdjustment,
        ConcurrencyRecommendation,
        ConcurrencyState,
        DeviceStatus,
        Metric,
        MetricStatus,
        ProviderInfo,
        ResourceLease,
        ResourceSnapshot,
        Scope,
        WorkloadProfile,
        WorkloadRequirements,
    )

_EXPORTS = {
    "AcceleratorDevice": "models",
    "AdmissionDecision": "models",
    "AdmissionRejectedError": "exceptions",
    "CapacityResult": "models",
    "ConcurrencyAdjustment": "models",
    "ConcurrencyController": "concurrency",
    "ConcurrencyRecommendation": "models",
    "ConcurrencySlot": "concurrency",
    "ConcurrencyState": "models",
    "DeviceReading": "accelerators",
    "DeviceStatus": "models",
    "ManagerStateError": "exceptions",
    "Metric": "models",
    "MetricStatus": "models",
    "OverheadBudgetError": "exceptions",
    "ProviderError": "exceptions",
    "ProviderInfo": "models",
    "ProviderReport": "accelerators",
    "ReservationError": "exceptions",
    "ResourceConfig": "config",
    "ResourceError": "exceptions",
    "ResourceManager": "manager",
    "ResourceLease": "models",
    "ResourceSnapshot": "models",
    "Scope": "models",
    "UnsupportedScopeError": "exceptions",
    "WorkloadProfile": "models",
    "WorkloadRequirements": "models",
}

__all__ = [
    "AcceleratorDevice",
    "AdmissionDecision",
    "AdmissionRejectedError",
    "CapacityResult",
    "ConcurrencyAdjustment",
    "ConcurrencyController",
    "ConcurrencyRecommendation",
    "ConcurrencySlot",
    "ConcurrencyState",
    "DeviceReading",
    "DeviceStatus",
    "ManagerStateError",
    "Metric",
    "MetricStatus",
    "OverheadBudgetError",
    "ProviderError",
    "ProviderInfo",
    "ProviderReport",
    "ReservationError",
    "ResourceConfig",
    "ResourceError",
    "ResourceLease",
    "ResourceManager",
    "ResourceSnapshot",
    "Scope",
    "UnsupportedScopeError",
    "WorkloadProfile",
    "WorkloadRequirements",
    "__version__",
]


def __getattr__(name: str) -> object:
    """Load public symbols only when an application requests them."""
    if name == "__version__":
        from importlib.metadata import PackageNotFoundError, version

        try:
            value = version("resourcera")
        except PackageNotFoundError:
            value = "0.2.0"
    elif name in _EXPORTS:
        value = getattr(import_module(f"resourcera.{_EXPORTS[name]}"), name)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
