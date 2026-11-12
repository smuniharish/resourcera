"""Validated configuration for resource monitoring and admission."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from resourcera.models import Scope


class ResourceConfig(BaseModel):
    """Configuration is explicit and does not read process environment variables."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    scope: Scope = Scope.MACHINE
    sampling: Literal["hybrid"] = "hybrid"
    overhead_policy: Literal["strict"] = "strict"
    cpu_memory_interval_seconds: float = Field(default=2.0, gt=0)
    disk_network_interval_seconds: float = Field(default=10.0, gt=0)
    sample_process_connections: bool = False
    inventory_interval_seconds: float = Field(default=300.0, gt=0)
    provider_timeout_seconds: float = Field(default=5.0, gt=0)
    cpu_budget_fraction: float = Field(default=0.01, gt=0, le=1)
    cpu_budget_window_seconds: float = Field(default=60.0, gt=0)
    managed_state_budget_bytes: int = Field(default=16 * 1024 * 1024, gt=0)
    safety_margin: float = Field(default=0.1, ge=0, lt=1)
    disk_read_capacity_bytes_per_second: float | None = Field(default=None, gt=0)
    disk_write_capacity_bytes_per_second: float | None = Field(default=None, gt=0)
    network_capacity_bytes_per_second: float | None = Field(default=None, gt=0)
    max_provider_concurrency: int = Field(default=1, ge=1)
    max_concurrency: int = Field(default=64, ge=1)
    max_waiters: int = Field(default=128, ge=1)
    admission_wait_timeout_seconds: float = Field(default=5.0, gt=0)
    lease_duration_seconds: float = Field(default=30.0, ge=0.3)
    coordination_database: Path | None = None
    accelerator_providers: tuple[str, ...] = Field(default=(), max_length=8)

    @field_validator("coordination_database")
    @classmethod
    def validate_coordination_database(cls, path: Path | None) -> Path | None:
        """Require an explicit local file, not SQLite's private memory database."""
        if path is not None and (str(path) == ":memory:" or not path.is_absolute()):
            raise ValueError("coordination_database must be an absolute local file path")
        return path

    @field_validator("accelerator_providers")
    @classmethod
    def validate_provider_paths(cls, paths: tuple[str, ...]) -> tuple[str, ...]:
        """Require importable module:callable names rather than executable paths."""
        for path in paths:
            parts = path.split(":")
            if (
                len(parts) != 2
                or not all(part.isidentifier() for part in parts[0].split("."))
                or not parts[1].isidentifier()
            ):
                raise ValueError(f"invalid accelerator provider: {path!r}")
        return paths
