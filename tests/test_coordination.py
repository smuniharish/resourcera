import multiprocessing
import os
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from uuid import uuid4

import pytest

from resourcera import (
    DeviceReading,
    DeviceStatus,
    ReservationError,
    ResourceConfig,
    WorkloadRequirements,
)
from resourcera.accelerators import normalize_device
from resourcera.coordination import ReservationStore
from resourcera.metrics import SampleClock
from resourcera.models import ResourceSnapshot


def _attempt_child(config_json, snapshot_json, ready, go, results, index):
    config = ResourceConfig.model_validate_json(config_json)
    snapshot = ResourceSnapshot.model_validate_json(snapshot_json)
    store = ReservationStore(config)
    store.initialize()
    ready.put(index)
    if not go.wait(10):
        raise RuntimeError("parent did not start the contention test")
    owner = f"owner-{index}"
    lease_id = f"lease-{index}"
    decision, _lease, _created = store.acquire(
        snapshot, WorkloadRequirements(memory="1GB"), owner, lease_id
    )
    results.put((decision.accepted, owner, lease_id))


def _crash_child(config_json, snapshot_json, channel):
    config = ResourceConfig.model_validate_json(config_json)
    snapshot = ResourceSnapshot.model_validate_json(snapshot_json)
    store = ReservationStore(config)
    store.initialize()
    decision, lease, _created = store.acquire(
        snapshot, WorkloadRequirements(memory="1GB"), "crashed-owner", "crashed-lease"
    )
    channel.send((decision.accepted, lease is not None))
    channel.close()
    os._exit(23)


def config_for(path: Path, *, duration: float = 3) -> ResourceConfig:
    return ResourceConfig(
        coordination_database=path,
        max_concurrency=1,
        lease_duration_seconds=duration,
    )


def test_process_local_concurrent_acquisition_is_atomic(snapshot: ResourceSnapshot) -> None:
    config = ResourceConfig(max_concurrency=3)
    store = ReservationStore(config)
    request = WorkloadRequirements(memory="100MB")
    owner = str(uuid4())

    def attempt(index: int):
        return store.acquire(snapshot, request, owner, f"{owner}-{index}")

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(attempt, range(16)))
    leases = tuple(lease.id for decision, lease, _ in results if decision.accepted and lease)
    assert len(leases) == 3
    assert store.release(owner, leases) == frozenset(leases)


def test_accelerator_uuid_prevents_duplicate_devices_and_alias_reuse(
    snapshot: ResourceSnapshot,
) -> None:
    clock = SampleClock.now()
    devices = tuple(
        normalize_device(
            DeviceReading(
                id=identifier,
                uuid="GPU-same",
                vendor="NVIDIA",
                kind="gpu",
                source="test",
                statuses=(DeviceStatus.TELEMETRY_AVAILABLE,),
                memory_total_bytes=8_000_000_000,
                memory_used_bytes=1_000_000_000,
            ),
            clock,
            60,
        )
        for identifier in ("first-provider", "second-provider")
    )
    store = ReservationStore(ResourceConfig(max_concurrency=2))
    both = snapshot.model_copy(update={"accelerators": devices})
    two = WorkloadRequirements(
        accelerator_type="gpu", accelerator_memory="1GB", accelerator_count=2
    )
    assert store.capacity(both, two).max_concurrency == 0
    one = two.model_copy(update={"accelerator_count": 1})
    owner = str(uuid4())
    decision, lease, _ = store.acquire(both, one, owner, owner)
    assert decision.accepted and lease is not None
    assert lease.accelerator_uuids == ("gpu-same",)
    alias_only = snapshot.model_copy(update={"accelerators": (devices[1],)})
    assert store.capacity(alias_only, one).max_concurrency == 0
    store.release(owner, (owner,))


def test_sqlite_acquisition_is_atomic_across_independent_processes(
    snapshot: ResourceSnapshot, tmp_path: Path
) -> None:
    config = config_for(tmp_path / "shared.sqlite")
    store = ReservationStore(config)
    store.initialize()
    context = multiprocessing.get_context("spawn")
    ready = context.Queue()
    results = context.Queue()
    go = context.Event()
    config_json = config.model_dump_json()
    snapshot_json = snapshot.model_dump_json(exclude_computed_fields=True)
    processes = [
        context.Process(
            target=_attempt_child,
            args=(config_json, snapshot_json, ready, go, results, index),
        )
        for index in range(2)
    ]
    for process in processes:
        process.start()
    try:
        assert {ready.get(timeout=15), ready.get(timeout=15)} == {0, 1}
        go.set()
        outcomes = [results.get(timeout=15), results.get(timeout=15)]
        for process in processes:
            process.join(15)
            assert process.exitcode == 0
        assert sum(accepted for accepted, _, _ in outcomes) == 1
        accepted = next(item for item in outcomes if item[0])
        assert store.release(accepted[1], (accepted[2],)) == frozenset({accepted[2]})
    finally:
        go.set()
        for process in processes:
            process.join(15)
        ready.close()
        results.close()


def test_sqlite_crashed_owner_expires_and_capacity_recovers(
    snapshot: ResourceSnapshot, tmp_path: Path
) -> None:
    config = config_for(tmp_path / "crash.sqlite", duration=1)
    store = ReservationStore(config)
    store.initialize()
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(
        target=_crash_child,
        args=(
            config.model_dump_json(),
            snapshot.model_dump_json(exclude_computed_fields=True),
            sender,
        ),
    )
    process.start()
    sender.close()
    try:
        assert receiver.poll(15)
        assert receiver.recv() == (True, True)
        process.join(15)
        assert process.exitcode == 23
        request = WorkloadRequirements(memory="1GB")
        denied, _, _ = store.acquire(snapshot, request, "new-owner", "new-lease")
        assert not denied.accepted
        time.sleep(1.1)
        accepted, lease, _ = store.acquire(snapshot, request, "new-owner", "new-lease")
        assert accepted.accepted and lease is not None
        store.release("new-owner", ("new-lease",))
    finally:
        receiver.close()
        process.join(15)


def test_sqlite_reboot_and_policy_mismatch_are_handled_safely(
    snapshot: ResourceSnapshot, tmp_path: Path
) -> None:
    path = tmp_path / "restart.sqlite"
    config = config_for(path)
    store = ReservationStore(config)
    store.initialize()
    request = WorkloadRequirements(memory="1GB")
    decision, lease, _ = store.acquire(snapshot, request, "owner", "old")
    assert decision.accepted and lease is not None

    incompatible = ReservationStore(config.model_copy(update={"max_concurrency": 2}))
    incompatible.initialize()
    with pytest.raises(ReservationError, match="different coordination policy"):
        incompatible.acquire(snapshot, request, "another", "new")

    with closing(sqlite3.connect(path)) as connection:
        connection.execute("UPDATE resourcera_meta SET boot_id = ?", (str(uuid4()),))
        connection.commit()
    result = store.capacity(snapshot, request)
    assert result.max_concurrency == 1
    accepted, new, _ = store.acquire(snapshot, request, "owner", "new")
    assert accepted.accepted and new is not None
    store.release("owner", ("new",))
