import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.examples
@pytest.mark.parametrize("example", sorted((ROOT / "examples").glob("*.py")), ids=lambda p: p.stem)
def test_executable_examples(example: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(example)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, f"{example.name}\n{result.stdout}\n{result.stderr}"
    assert result.stdout.strip(), example.name
    if example.name == "custom_provider.py":
        assert "Provider: Linux accelerator class example" in result.stdout, result.stdout


@pytest.mark.examples
@pytest.mark.parametrize(
    "document", [ROOT / "README.md", ROOT / "docs" / "getting-started" / "quickstart.md"]
)
def test_documented_quickstart(document: Path) -> None:
    source = document.read_text(encoding="utf-8")
    snippets = re.findall(r"```python\n(.*?)\n```", source, flags=re.DOTALL)
    assert snippets, f"No Python quickstart found in {document}"
    result = subprocess.run(
        [sys.executable, "-c", snippets[0]],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, f"{document}\n{result.stdout}\n{result.stderr}"
    assert result.stdout.strip()
