"""Exceptions surfaced by RESOURCERA."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from resourcera.models import AdmissionDecision


class ResourceError(Exception):
    """Base class for library errors."""


class ManagerStateError(ResourceError):
    """A manager operation is incompatible with its lifecycle state."""


class UnsupportedScopeError(ResourceError):
    """A requested resource scope has no implemented backend."""


class ProviderError(ResourceError):
    """A resource provider failed or timed out."""


class OverheadBudgetError(ResourceError):
    """An optional probe was refused by the active overhead policy."""


class ReservationError(ResourceError):
    """A reservation could not be acquired, renewed, or released safely."""


class AdmissionRejectedError(ResourceError):
    """A resource-aware admission request was rejected."""

    def __init__(self, decision: AdmissionDecision) -> None:
        super().__init__(decision.reason)
        self.decision = decision
