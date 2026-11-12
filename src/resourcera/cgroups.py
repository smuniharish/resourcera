"""Read Linux cgroup limits without modifying the host."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from resourcera.models import LimitState


@dataclass(frozen=True, slots=True)
class CgroupLimit:
    """A finite, explicitly unlimited, unknown, or inapplicable limit."""

    state: LimitState
    value: int | float | None = None
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class CgroupMeasurements:
    """Limits and usage attributable to the current cgroup hierarchy."""

    cpu: CgroupLimit
    memory: CgroupLimit
    memory_available: int | None = None
    memory_usage: int | None = None
    cpu_usage_seconds: float | None = None
    throttled_seconds: float | None = None
    cpu_stats_detail: str | None = None
    container_id: str | None = None
    cpu_quota_domains: tuple[CpuQuotaDomain, ...] = ()


@dataclass(frozen=True, slots=True)
class CpuQuotaDomain:
    """A finite quota and cumulative consumption for one cgroup ancestor."""

    path: str
    quota_cores: float
    usage_seconds: float | None
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class _Mount:
    mountpoint: Path
    group_path: Path


def _mount_path(mountpoint: str, root: str, group: str) -> _Mount | None:
    try:
        relative = PurePosixPath(group).relative_to(PurePosixPath(root))
    except ValueError:
        return None
    mount = Path(mountpoint.replace("\\040", " ").replace("\\011", "\t"))
    return _Mount(mount, mount.joinpath(*relative.parts))


def _find_mounts(cgroup_text: str, mountinfo_text: str) -> tuple[_Mount | None, _Mount | None, int]:
    groups: dict[str, str] = {}
    for line in cgroup_text.splitlines():
        fields = line.split(":", 2)
        if len(fields) != 3:
            continue
        controllers = fields[1].split(",") if fields[1] else ["unified"]
        for controller in controllers:
            groups[controller] = fields[2]

    cpu: _Mount | None = None
    memory: _Mount | None = None
    version = 0
    for line in mountinfo_text.splitlines():
        left, separator, right = line.partition(" - ")
        if not separator:
            continue
        fields = left.split()
        filesystem = right.split()
        if len(fields) < 5 or len(filesystem) < 3:
            continue
        kind = filesystem[0]
        if kind == "cgroup2" and "unified" in groups:
            mount = _mount_path(fields[4], fields[3], groups["unified"])
            if mount is not None:
                return mount, mount, 2
            if version == 0:
                version = 2
            continue
        if kind != "cgroup":
            continue
        controllers = set(filesystem[2].split(","))
        if "cpu" in controllers and "cpu" in groups:
            mount = _mount_path(fields[4], fields[3], groups["cpu"])
            if mount is not None:
                cpu = mount
                version = 1
        if "memory" in controllers and "memory" in groups:
            mount = _mount_path(fields[4], fields[3], groups["memory"])
            if mount is not None:
                memory = mount
                version = 1
    return cpu, memory, version


def _ancestors(mount: _Mount) -> list[Path]:
    try:
        mount.group_path.relative_to(mount.mountpoint)
    except ValueError as exc:
        raise ValueError("cgroup path is outside its mount") from exc
    current = mount.group_path
    result = [current]
    while current != mount.mountpoint:
        current = current.parent
        result.append(current)
    return result


def _cpu_quota_domains(
    mount: _Mount, version: int
) -> tuple[CgroupLimit, tuple[CpuQuotaDomain, ...]]:
    limits: list[float] = []
    domains: list[CpuQuotaDomain] = []
    try:
        for path in _ancestors(mount):
            if version == 2:
                quota, period = (path / "cpu.max").read_text(encoding="utf-8").split()
            else:
                quota = (path / "cpu.cfs_quota_us").read_text(encoding="utf-8").strip()
                period = (path / "cpu.cfs_period_us").read_text(encoding="utf-8").strip()
            if quota in {"max", "-1"}:
                continue
            quota_value, period_value = int(quota), int(period)
            if quota_value <= 0 or period_value <= 0:
                raise ValueError("invalid CPU quota or period")
            quota_cores = quota_value / period_value
            limits.append(quota_cores)
            usage, _, detail = _cpu_stats(_Mount(mount.mountpoint, path), version)
            domains.append(CpuQuotaDomain(str(path), quota_cores, usage, detail))
    except (OSError, ValueError) as exc:
        return CgroupLimit(LimitState.UNKNOWN, detail=str(exc)), ()
    if limits:
        return CgroupLimit(LimitState.FINITE, min(limits)), tuple(domains)
    return CgroupLimit(LimitState.UNLIMITED), ()


def _cpu_limit(mount: _Mount, version: int) -> CgroupLimit:
    return _cpu_quota_domains(mount, version)[0]


def _memory_limit(mount: _Mount, version: int) -> tuple[CgroupLimit, int | None, int | None]:
    limits: list[int] = []
    available: list[int] = []
    usage: int | None = None
    try:
        for index, path in enumerate(_ancestors(mount)):
            limit_name = "memory.max" if version == 2 else "memory.limit_in_bytes"
            usage_name = "memory.current" if version == 2 else "memory.usage_in_bytes"
            raw_limit = (path / limit_name).read_text(encoding="utf-8").strip()
            current = int((path / usage_name).read_text(encoding="utf-8").strip())
            if current < 0:
                raise ValueError("negative cgroup memory usage")
            if index == 0:
                usage = current
            if raw_limit == "max":
                continue
            limit = int(raw_limit)
            if limit < 0:
                raise ValueError("negative cgroup memory limit")
            if version == 1 and limit >= 1 << 60:
                continue
            limits.append(limit)
            available.append(max(0, limit - current))
    except (OSError, ValueError) as exc:
        return CgroupLimit(LimitState.UNKNOWN, detail=str(exc)), None, usage
    if limits:
        return CgroupLimit(LimitState.FINITE, min(limits)), min(available), usage
    return CgroupLimit(LimitState.UNLIMITED), None, usage


def _cpu_stats(mount: _Mount, version: int) -> tuple[float | None, float | None, str | None]:
    usage: float | None = None
    throttle: float | None = None
    errors: list[str] = []
    try:
        entries = dict(
            line.split(maxsplit=1)
            for line in (mount.group_path / "cpu.stat").read_text(encoding="utf-8").splitlines()
        )
    except (OSError, ValueError) as exc:
        entries = {}
        errors.append(str(exc))
    key, scale = ("throttled_usec", 1_000_000) if version == 2 else ("throttled_time", 1e9)
    if key in entries:
        try:
            throttle = int(entries[key]) / scale
        except ValueError as exc:
            errors.append(str(exc))
    if version == 2 and "usage_usec" in entries:
        try:
            usage = int(entries["usage_usec"]) / 1_000_000
        except ValueError as exc:
            errors.append(str(exc))
    if version == 1:
        try:
            usage = int((mount.group_path / "cpuacct.usage").read_text(encoding="utf-8")) / 1e9
        except (OSError, ValueError) as exc:
            errors.append(str(exc))
    if usage is None and not errors:
        errors.append("cgroup CPU usage counter is unavailable")
    return usage, throttle, "; ".join(errors) if errors else None


def read_cgroup() -> CgroupMeasurements:
    """Read the current process's effective Linux cgroup limits."""
    if sys.platform != "linux":
        not_applicable = CgroupLimit(LimitState.NOT_APPLICABLE)
        return CgroupMeasurements(not_applicable, not_applicable)
    cgroup_text = ""
    mountinfo = ""
    try:
        cgroup_text = Path("/proc/self/cgroup").read_text(encoding="utf-8")
        mountinfo = Path("/proc/self/mountinfo").read_text(encoding="utf-8")
    except OSError as exc:
        unknown = CgroupLimit(LimitState.UNKNOWN, detail=str(exc))
        return CgroupMeasurements(unknown, unknown)

    cpu_mount, memory_mount, version = _find_mounts(cgroup_text, mountinfo)
    unknown = CgroupLimit(LimitState.UNKNOWN, detail="cgroup controller mount is unavailable")
    cpu, quota_domains = _cpu_quota_domains(cpu_mount, version) if cpu_mount else (unknown, ())
    if memory_mount:
        memory, available, usage = _memory_limit(memory_mount, version)
    else:
        memory, available, usage = unknown, None, None
    cpu_usage, throttle, stats_detail = (
        _cpu_stats(cpu_mount, version) if cpu_mount else (None, None, unknown.detail)
    )
    group_name = memory_mount.group_path.name if memory_mount else None
    if memory_mount and memory_mount.group_path == memory_mount.mountpoint:
        group_name = None
    return CgroupMeasurements(
        cpu, memory, available, usage, cpu_usage, throttle, stats_detail, group_name, quota_domains
    )
