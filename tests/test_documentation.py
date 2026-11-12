import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _diagram_fixture(destination: Path) -> None:
    shutil.copytree(ROOT / "diagrams", destination / "diagrams")
    shutil.copytree(
        ROOT / "docs" / "assets" / "diagrams", destination / "docs" / "assets" / "diagrams"
    )
    (destination / "docs" / "architecture").mkdir(parents=True)
    shutil.copy2(
        ROOT / "docs" / "architecture" / "diagrams.md", destination / "docs" / "architecture"
    )
    (destination / "scripts").mkdir()
    shutil.copy2(ROOT / "scripts" / "render-diagrams.mjs", destination / "scripts")
    for name in ("package.json", "package-lock.json"):
        shutil.copy2(ROOT / name, destination)


@pytest.mark.parametrize(
    "change", ["source", "config", "image", "reference", "renderer", "missing"]
)
def test_diagram_checker_detects_stale_or_missing_inputs(tmp_path: Path, change: str) -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for diagram validation")
    _diagram_fixture(tmp_path)
    command = [node, str(tmp_path / "scripts" / "render-diagrams.mjs"), "--check"]
    baseline = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True, timeout=30)
    assert baseline.returncode == 0, baseline.stderr
    if change == "source":
        path = tmp_path / "diagrams" / "capacity-estimation.mmd"
        path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    elif change == "config":
        path = tmp_path / "diagrams" / "mermaid-config.json"
        path.write_text(path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    elif change == "image":
        path = tmp_path / "docs" / "assets" / "diagrams" / "capacity-estimation.png"
        data = bytearray(path.read_bytes())
        data[-1] ^= 1
        path.write_bytes(data)
    elif change == "reference":
        path = tmp_path / "docs" / "architecture" / "diagrams.md"
        text = path.read_text(encoding="utf-8")
        path.write_text(
            text.replace("assets/diagrams/capacity-estimation.png", "missing.png"), encoding="utf-8"
        )
    elif change == "renderer":
        path = tmp_path / "package.json"
        text = path.read_text(encoding="utf-8")
        path.write_text(text.replace('"12.2.0"', '"12.2.1"'), encoding="utf-8")
    else:
        (tmp_path / "diagrams" / "capacity-estimation.mmd").unlink()
    result = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True, timeout=30)
    assert result.returncode != 0, change


@pytest.mark.parametrize("reference", ["/en/latest/manual/#missing", "/en/latest/missing.png"])
def test_site_link_checker_detects_missing_targets(tmp_path: Path, reference: str) -> None:
    (tmp_path / "scripts").mkdir()
    shutil.copy2(ROOT / "scripts" / "check_site_links.py", tmp_path / "scripts")
    (tmp_path / "zensical.toml").write_text(
        '[project]\nsite_url = "https://example.org/en/latest/"\n', encoding="utf-8"
    )
    (tmp_path / "site" / "manual").mkdir(parents=True)
    (tmp_path / "site" / "manual" / "index.html").write_text(
        '<h1 id="present">Manual</h1>', encoding="utf-8"
    )
    index = tmp_path / "site" / "index.html"
    index.write_text('<a href="/en/latest/manual/#present">Manual</a>', encoding="utf-8")
    command = [sys.executable, str(tmp_path / "scripts" / "check_site_links.py")]
    baseline = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True, timeout=30)
    assert baseline.returncode == 0, baseline.stderr
    index.write_text(f'<a href="{reference}">Broken</a>', encoding="utf-8")
    result = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True, timeout=30)
    assert result.returncode != 0
    assert "missing" in result.stderr
