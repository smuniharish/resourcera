"""Atomic logical reservations for one process or a shared local SQLite file."""

from __future__ import annotations

import ctypes
import json
import socket
import sqlite3
import sys
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pydantic import ValidationError

from resourcera.capacity import ResourceClaims, estimate_capacity
from resourcera.config import ResourceConfig
from resourcera.exceptions import ReservationError
from resourcera.models import (
    AdmissionDecision,
    CapacityResult,
    ResourceLease,
    ResourceSnapshot,
    Scope,
    WorkloadRequirements,
)


@dataclass(frozen=True, slots=True)
class _Record:
    lease: ResourceLease
    policy: str


class _Records:
    def __init__(self, records: dict[str, _Record]) -> None:
        self.records = records
        self.changed: set[str] = set()
        self.deleted: set[str] = set()

    def put(self, record: _Record) -> None:
        self.records[record.lease.id] = record
        self.changed.add(record.lease.id)
        self.deleted.discard(record.lease.id)

    def remove(self, lease_id: str) -> None:
        self.records.pop(lease_id, None)
        self.changed.discard(lease_id)
        self.deleted.add(lease_id)


_PROCESS_LOCK = threading.Lock()
_PROCESS_RECORDS: dict[str, _Record] = {}


def _policy(config: ResourceConfig) -> str:
    return json.dumps(
        {
            "scope": config.scope.value,
            "max_concurrency": config.max_concurrency,
            "safety_margin": config.safety_margin,
            "disk_read_capacity": config.disk_read_capacity_bytes_per_second,
            "disk_write_capacity": config.disk_write_capacity_bytes_per_second,
            "network_capacity": config.network_capacity_bytes_per_second,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _claims(records: dict[str, _Record]) -> ResourceClaims:
    cpu = disk_read = disk_write = network = 0.0
    memory = 0
    devices: set[str] = set()
    for record in records.values():
        request = record.lease.requirements
        cpu += request.cpu or 0
        memory += int(request.memory) if request.memory is not None else 0
        disk_read += request.disk_read_bytes_per_second or 0
        disk_write += request.disk_write_bytes_per_second or 0
        network += request.network_bytes_per_second or 0
        devices.update(record.lease.accelerator_devices)
        devices.update(value.casefold() for value in record.lease.accelerator_uuids)
    return ResourceClaims(cpu, memory, disk_read, disk_write, network, frozenset(devices))


def _estimate(
    records: dict[str, _Record],
    snapshot: ResourceSnapshot,
    config: ResourceConfig,
    requirements: WorkloadRequirements | None,
    now: float,
) -> CapacityResult:
    if records and any(record.policy != _policy(config) for record in records.values()):
        raise ReservationError("active reservations use a different coordination policy")
    request = requirements
    if request is not None:
        margins = [
            record.lease.requirements.safety_margin
            if record.lease.requirements.safety_margin is not None
            else config.safety_margin
            for record in records.values()
        ]
        margins.append(
            request.safety_margin if request.safety_margin is not None else config.safety_margin
        )
        request = request.model_copy(update={"safety_margin": max(margins)})
    result = estimate_capacity(snapshot, config, request, _claims(records), now=now)
    if result.max_concurrency is not None:
        remaining = max(0, config.max_concurrency - len(records))
        if remaining < result.max_concurrency:
            result = result.model_copy(
                update={
                    "max_concurrency": remaining,
                    "limiting_resource": "max_concurrency",
                }
            )
    return result


def _decision(
    capacity: CapacityResult,
    requirements: WorkloadRequirements,
    policy: str,
    lease_id: str,
) -> AdmissionDecision:
    accepted = capacity.max_concurrency is not None and capacity.max_concurrency > 0
    if capacity.max_concurrency is None:
        reason = f"Required {capacity.limiting_resource} capacity is unknown or stale."
    elif not accepted:
        reason = f"Insufficient {capacity.limiting_resource} capacity."
    else:
        reason = "Fresh required resources and a concurrency slot are available."
    return AdmissionDecision(
        accepted=accepted,
        requirements=requirements,
        capacity=capacity,
        policy=policy,
        reason=reason,
        lease_id=lease_id if accepted else None,
    )


def _boot_id() -> str:
    """Read an OS-provided boot-session UUID; never infer one from wall time."""
    try:
        if sys.platform.startswith("linux"):
            value = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
        elif sys.platform == "win32":

            class BootInformation(ctypes.Structure):
                _fields_ = [
                    ("identifier", ctypes.c_ubyte * 16),
                    ("firmware", ctypes.c_uint32),
                    ("flags", ctypes.c_uint64),
                ]

            info = BootInformation()
            returned = ctypes.c_uint32()
            ntdll = ctypes.WinDLL("ntdll")  # type: ignore[attr-defined]
            query = ntdll.NtQuerySystemInformation
            query.argtypes = [
                ctypes.c_uint32,
                ctypes.c_void_p,
                ctypes.c_uint32,
                ctypes.POINTER(ctypes.c_uint32),
            ]
            query.restype = ctypes.c_int32
            status = query(90, ctypes.byref(info), ctypes.sizeof(info), ctypes.byref(returned))
            if status != 0 or returned.value < ctypes.sizeof(info):
                raise OSError(f"boot-session query failed with NTSTATUS {status:#x}")
            value = str(uuid.UUID(bytes_le=bytes(info.identifier)))
        elif sys.platform == "darwin":
            libc = ctypes.CDLL(None, use_errno=True)
            query = libc.sysctlbyname
            query.argtypes = [
                ctypes.c_char_p,
                ctypes.c_void_p,
                ctypes.POINTER(ctypes.c_size_t),
                ctypes.c_void_p,
                ctypes.c_size_t,
            ]
            query.restype = ctypes.c_int
            buffer = ctypes.create_string_buffer(64)
            size = ctypes.c_size_t(len(buffer))
            if query(b"kern.bootsessionuuid", buffer, ctypes.byref(size), None, 0) != 0:
                raise OSError(ctypes.get_errno(), "boot-session query failed")
            value = buffer.value.decode("ascii")
        else:
            raise OSError(f"boot identity is unsupported on {sys.platform}")
        return str(uuid.UUID(value))
    except (OSError, ValueError, AttributeError) as exc:
        raise ReservationError(f"cannot establish a reliable boot identity: {exc}") from exc


class ReservationStore:
    """Execute small atomic transactions in an AnyIO worker thread."""

    def __init__(self, config: ResourceConfig) -> None:
        self.config = config
        self.path = config.coordination_database
        self.scope = Scope.MACHINE if self.path is not None else Scope.PROCESS
        self.policy_name = "machine-local SQLite" if self.path is not None else "process-local"
        self._boot: str | None = None
        self._hostname: str | None = None

    def initialize(self) -> None:
        """Create and validate the SQLite store when configured."""
        if self.path is not None:
            self._boot = _boot_id()
            self._hostname = socket.gethostname()
            self._transact(lambda _records, _now: None)

    def _transact[T](self, operation: Callable[[_Records, float], T]) -> T:
        if self.path is None:
            with _PROCESS_LOCK:
                now = time.monotonic()
                state = _Records(dict(_PROCESS_RECORDS))
                for lease_id, record in tuple(state.records.items()):
                    if record.lease.expires_monotonic <= now:
                        state.remove(lease_id)
                result = operation(state, now)
                _PROCESS_RECORDS.clear()
                _PROCESS_RECORDS.update(state.records)
                return result

        if self._boot is None or self._hostname is None:
            raise ReservationError("SQLite reservation store has not been initialized")
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(self.path, timeout=0.25, isolation_level=None)
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS resourcera_meta "
                "(id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL, "
                "hostname TEXT NOT NULL, boot_id TEXT NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS resourcera_leases "
                "(id TEXT PRIMARY KEY, expires REAL NOT NULL, lease TEXT NOT NULL, "
                "policy TEXT NOT NULL)"
            )
            metadata = connection.execute(
                "SELECT version, hostname, boot_id FROM resourcera_meta WHERE id = 1"
            ).fetchone()
            if metadata is None:
                connection.execute(
                    "INSERT INTO resourcera_meta VALUES (1, 1, ?, ?)",
                    (self._hostname, self._boot),
                )
            else:
                version, hostname, boot = metadata
                if version != 1 or hostname != self._hostname:
                    raise ReservationError("SQLite store belongs to another host or schema")
                if boot != self._boot:
                    connection.execute("DELETE FROM resourcera_leases")
                    connection.execute(
                        "UPDATE resourcera_meta SET boot_id = ? WHERE id = 1", (self._boot,)
                    )
            now = time.monotonic()
            connection.execute("DELETE FROM resourcera_leases WHERE expires <= ?", (now,))
            rows = connection.execute(
                "SELECT id, expires, lease, policy FROM resourcera_leases"
            ).fetchall()
            records: dict[str, _Record] = {}
            for lease_id, expires, payload, policy in rows:
                lease = ResourceLease.model_validate_json(payload)
                if lease.id != lease_id or lease.expires_monotonic != expires:
                    raise ReservationError("SQLite reservation data is inconsistent")
                records[lease_id] = _Record(lease, policy)
            state = _Records(records)
            result = operation(state, now)
            for lease_id in state.deleted:
                connection.execute("DELETE FROM resourcera_leases WHERE id = ?", (lease_id,))
            for lease_id in state.changed:
                record = state.records[lease_id]
                connection.execute(
                    "INSERT INTO resourcera_leases (id, expires, lease, policy) "
                    "VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(id) DO UPDATE SET expires = excluded.expires, "
                    "lease = excluded.lease, policy = excluded.policy",
                    (
                        lease_id,
                        record.lease.expires_monotonic,
                        record.lease.model_dump_json(exclude_computed_fields=True),
                        record.policy,
                    ),
                )
            connection.execute("COMMIT")
            return result
        except (sqlite3.Error, ValidationError) as exc:
            raise ReservationError(f"SQLite coordination failed: {exc}") from exc
        finally:
            if connection is not None:
                connection.close()

    def capacity(
        self, snapshot: ResourceSnapshot, requirements: WorkloadRequirements | None
    ) -> CapacityResult:
        """Read current claims and calculate remaining capacity atomically."""
        return self._transact(
            lambda state, now: _estimate(state.records, snapshot, self.config, requirements, now)
        )

    def acquire(
        self,
        snapshot: ResourceSnapshot,
        requirements: WorkloadRequirements,
        owner_id: str,
        lease_id: str,
    ) -> tuple[AdmissionDecision, ResourceLease | None, bool]:
        """Check claims and insert a lease in the same transaction."""

        def acquire_in_transaction(
            state: _Records, now: float
        ) -> tuple[AdmissionDecision, ResourceLease | None, bool]:
            existing = state.records.get(lease_id)
            if existing is not None:
                lease = existing.lease
                if lease.owner_id != owner_id or lease.requirements != requirements:
                    raise ReservationError("lease ID is already owned by a different reservation")
                return lease.decision, lease, False
            capacity = _estimate(state.records, snapshot, self.config, requirements, now)
            decision = _decision(capacity, requirements, self.policy_name, lease_id)
            if not decision.accepted:
                return decision, None, False
            needs_device = any(
                value is not None
                for value in (
                    requirements.accelerator_type,
                    requirements.accelerator_count,
                    requirements.accelerator_memory,
                )
            )
            count = requirements.accelerator_count or 1
            devices = capacity.accelerator_devices[:count] if needs_device else ()
            uuids = tuple(
                sorted(
                    {
                        device.uuid.casefold()
                        for device in snapshot.accelerators
                        if device.id in devices and device.uuid is not None
                    }
                )
            )
            lease = ResourceLease(
                id=lease_id,
                owner_id=owner_id,
                scope=self.scope,
                expires_at=datetime.now(UTC)
                + timedelta(seconds=self.config.lease_duration_seconds),
                expires_monotonic=now + self.config.lease_duration_seconds,
                requirements=requirements,
                accelerator_devices=devices,
                accelerator_uuids=uuids,
                decision=decision,
            )
            state.put(_Record(lease, _policy(self.config)))
            return decision, lease, True

        return self._transact(acquire_in_transaction)

    def renew(self, owner_id: str, lease_ids: tuple[str, ...]) -> dict[str, ResourceLease]:
        """Renew existing leases belonging to one owner without reviving expired leases."""

        def renew_in_transaction(state: _Records, now: float) -> dict[str, ResourceLease]:
            renewed: dict[str, ResourceLease] = {}
            for lease_id in lease_ids:
                record = state.records.get(lease_id)
                if record is None:
                    continue
                if record.lease.owner_id != owner_id:
                    raise ReservationError("cannot renew another owner's lease")
                lease = record.lease.model_copy(
                    update={
                        "expires_monotonic": now + self.config.lease_duration_seconds,
                        "expires_at": datetime.now(UTC)
                        + timedelta(seconds=self.config.lease_duration_seconds),
                    }
                )
                state.put(_Record(lease, record.policy))
                renewed[lease_id] = lease
            return renewed

        return self._transact(renew_in_transaction)

    def release(self, owner_id: str, lease_ids: tuple[str, ...]) -> frozenset[str]:
        """Delete owned leases atomically and return those that existed."""

        def release_in_transaction(state: _Records, _now: float) -> frozenset[str]:
            released: set[str] = set()
            for lease_id in lease_ids:
                record = state.records.get(lease_id)
                if record is not None:
                    if record.lease.owner_id != owner_id:
                        raise ReservationError("cannot release another owner's lease")
                    state.remove(lease_id)
                    released.add(lease_id)
            return frozenset(released)

        return self._transact(release_in_transaction)
