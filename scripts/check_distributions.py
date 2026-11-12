"""Verify required wheel and source-distribution contents."""

from __future__ import annotations

import argparse
import glob
import tarfile
import zipfile
from pathlib import Path


def _distribution_path(pattern: str) -> Path:
    matches = glob.glob(pattern)
    if len(matches) != 1:
        raise argparse.ArgumentTypeError(f"{pattern!r} must match exactly one distribution")
    return Path(matches[0])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=_distribution_path)
    parser.add_argument("sdist", type=_distribution_path)
    args = parser.parse_args()
    with zipfile.ZipFile(args.wheel) as archive:
        wheel = set(archive.namelist())
    with tarfile.open(args.sdist, "r:gz") as archive:
        source = {name.partition("/")[2] for name in archive.getnames() if "/" in name}
    expected_wheel = {"resourcera/__init__.py", "resourcera/manager.py", "resourcera/py.typed"}
    expected_source = {
        "pyproject.toml",
        "uv.lock",
        "src/resourcera/__init__.py",
        "docs/index.md",
        "docs/architecture/diagrams.md",
        "docs/assets/diagrams/manifest.json",
        "diagrams/overall-architecture.mmd",
        "examples/basic_snapshot.py",
        "examples/custom_provider.py",
        "tests/test_telemetry.py",
        "skills/resourcera/SKILL.md",
        "scripts/check_site_links.py",
        "zensical.toml",
        "package.json",
        "package-lock.json",
    }
    missing_wheel = sorted(expected_wheel - wheel)
    missing_source = sorted(expected_source - source)
    if missing_wheel or missing_source:
        raise SystemExit(
            f"Missing wheel files: {missing_wheel}; missing sdist files: {missing_source}"
        )
    print(f"Validated {len(wheel)} wheel entries and {len(source)} source entries.")


if __name__ == "__main__":
    main()
