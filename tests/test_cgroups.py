from pathlib import Path

from resourcera.cgroups import (
    _cpu_limit,
    _cpu_quota_domains,
    _cpu_stats,
    _find_mounts,
    _memory_limit,
    _Mount,
)
from resourcera.models import LimitState


def test_v2_hierarchy_uses_the_most_restrictive_ancestor(tmp_path: Path) -> None:
    child = tmp_path / "child"
    child.mkdir()
    (tmp_path / "cpu.max").write_text("400000 100000", encoding="utf-8")
    (child / "cpu.max").write_text("150000 100000", encoding="utf-8")
    (tmp_path / "memory.max").write_text("1000", encoding="utf-8")
    (tmp_path / "memory.current").write_text("700", encoding="utf-8")
    (child / "memory.max").write_text("800", encoding="utf-8")
    (child / "memory.current").write_text("200", encoding="utf-8")
    (child / "cpu.stat").write_text("usage_usec 2500000\nthrottled_usec 500000\n", encoding="utf-8")
    mount = _Mount(tmp_path, child)

    cpu = _cpu_limit(mount, 2)
    memory, available, usage = _memory_limit(mount, 2)
    assert (cpu.state, cpu.value) == (LimitState.FINITE, 1.5)
    assert (memory.state, memory.value, available, usage) == (LimitState.FINITE, 800, 300, 200)
    assert _cpu_stats(mount, 2) == (2.5, 0.5, None)


def test_unknown_limit_is_not_treated_as_unlimited(tmp_path: Path) -> None:
    mount = _Mount(tmp_path, tmp_path)
    assert _cpu_limit(mount, 2).state is LimitState.UNKNOWN
    assert _memory_limit(mount, 2)[0].state is LimitState.UNKNOWN
    (tmp_path / "cpu.max").write_text("max 100000", encoding="utf-8")
    (tmp_path / "memory.max").write_text("max", encoding="utf-8")
    (tmp_path / "memory.current").write_text("42", encoding="utf-8")
    assert _cpu_limit(mount, 2).state is LimitState.UNLIMITED
    assert _memory_limit(mount, 2)[0].state is LimitState.UNLIMITED


def test_v1_unlimited_sentinel_and_finite_quota(tmp_path: Path) -> None:
    mount = _Mount(tmp_path, tmp_path)
    (tmp_path / "cpu.cfs_quota_us").write_text("50000", encoding="utf-8")
    (tmp_path / "cpu.cfs_period_us").write_text("100000", encoding="utf-8")
    (tmp_path / "memory.limit_in_bytes").write_text(str(1 << 62), encoding="utf-8")
    (tmp_path / "memory.usage_in_bytes").write_text("123", encoding="utf-8")
    assert _cpu_limit(mount, 1).value == 0.5
    limit, available, usage = _memory_limit(mount, 1)
    assert (limit.state, available, usage) == (LimitState.UNLIMITED, None, 123)


def test_mount_root_maps_to_nested_group(tmp_path: Path) -> None:
    mountinfo = f"1 0 0:1 /tenant {tmp_path} rw - cgroup2 cgroup rw"
    cpu, memory, version = _find_mounts("0::/tenant/job\n", mountinfo)
    assert version == 2
    assert cpu == memory == _Mount(tmp_path, tmp_path / "job")


def test_unrelated_cgroup2_mount_does_not_hide_matching_mount(tmp_path: Path) -> None:
    unrelated = tmp_path / "unrelated"
    matching = tmp_path / "matching"
    mountinfo = (
        f"1 0 0:1 /other {unrelated} rw - cgroup2 cgroup rw\n"
        f"2 0 0:2 /tenant {matching} rw - cgroup2 cgroup rw"
    )
    cpu, memory, version = _find_mounts("0::/tenant/job\n", mountinfo)
    assert version == 2
    assert cpu == memory == _Mount(matching, matching / "job")


def test_unrelated_v1_mount_does_not_replace_matching_controller(tmp_path: Path) -> None:
    cpu_dir = tmp_path / "cpu"
    memory_dir = tmp_path / "memory"
    mountinfo = (
        f"1 0 0:1 /tenant {cpu_dir} rw - cgroup cgroup rw,cpu,cpuacct\n"
        f"2 0 0:2 /tenant {memory_dir} rw - cgroup cgroup rw,memory\n"
        f"3 0 0:3 /other {tmp_path / 'other'} rw - cgroup cgroup rw,cpu,memory"
    )
    cpu, memory, version = _find_mounts(
        "2:cpu,cpuacct:/tenant/job\n3:memory:/tenant/job\n", mountinfo
    )
    assert version == 1
    assert cpu == _Mount(cpu_dir, cpu_dir / "job")
    assert memory == _Mount(memory_dir, memory_dir / "job")


def test_cpu_quota_reads_ancestor_consumption_including_siblings(tmp_path: Path) -> None:
    child = tmp_path / "child"
    child.mkdir()
    (tmp_path / "cpu.max").write_text("200000 100000", encoding="utf-8")
    (tmp_path / "cpu.stat").write_text("usage_usec 20000000\n", encoding="utf-8")
    (child / "cpu.max").write_text("400000 100000", encoding="utf-8")
    (child / "cpu.stat").write_text("usage_usec 10000000\n", encoding="utf-8")
    quota, domains = _cpu_quota_domains(_Mount(tmp_path, child), 2)
    assert quota.value == 2
    assert [(item.quota_cores, item.usage_seconds) for item in domains] == [(4, 10), (2, 20)]
