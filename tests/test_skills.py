import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SKILLS = ROOT / "skills"
NAMES = ("resourcera", "resourcera-provider")


def test_manifest_discovers_both_complete_skill_directories() -> None:
    manifest = json.loads((ROOT / "skills.sh.json").read_text(encoding="utf-8"))
    roots = [(ROOT / path).resolve() for path in manifest["paths"]]
    discovered = {
        path.name
        for root in roots
        for path in root.iterdir()
        if path.is_dir() and (path / "SKILL.md").is_file()
    }
    assert discovered == set(NAMES)


@pytest.mark.parametrize("name", NAMES)
def test_skill_metadata_references_and_evaluations(name: str) -> None:
    skill = SKILLS / name
    text = (skill / "SKILL.md").read_text(encoding="utf-8")
    frontmatter = text.split("---", 2)[1]
    version = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"][
        "version"
    ]
    assert re.search(rf"(?m)^name: {re.escape(name)}$", frontmatter)
    assert f'version: "{version}"' in frontmatter
    assert "license: Apache-2.0" in frontmatter
    references = list((skill / "references").glob("*.md"))
    assert references
    for reference in references:
        assert f"](references/{reference.name})" in text
    for target in re.findall(r"\]\((references/[^)]+)\)", text):
        assert (skill / target).is_file()

    cases = json.loads((skill / "evals" / "evals.json").read_text(encoding="utf-8"))
    assert cases["skill_name"] == name
    assert len(cases["evals"]) >= 3
    assert len({case["id"] for case in cases["evals"]}) == len(cases["evals"])
    assert all(
        case["prompt"] and case["expected_output"] and case["assertions"] for case in cases["evals"]
    )
    triggers = json.loads((skill / "evals" / "trigger_queries.json").read_text(encoding="utf-8"))
    assert triggers["skill_name"] == name
    for split in ("train", "validation"):
        labels = {case["should_trigger"] for case in triggers["queries"] if case["split"] == split}
        assert labels == {True, False}


def test_skill_trigger_queries_have_distinct_positive_scopes() -> None:
    positives = []
    for name in NAMES:
        data = json.loads(
            (SKILLS / name / "evals" / "trigger_queries.json").read_text(encoding="utf-8")
        )
        positives.append(
            {query["query"].casefold() for query in data["queries"] if query["should_trigger"]}
        )
    assert not positives[0] & positives[1]


def test_setup_check_returns_json_and_closes_manager() -> None:
    script = SKILLS / "resourcera" / "scripts" / "verify_setup.py"
    result = subprocess.run(
        [sys.executable, str(script), "--json"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report["ok"] is True
    checks = {check["check"]: check for check in report["checks"]}
    assert checks["snapshot"]["status"] == "PASS"
    assert checks["capabilities"]["status"] == "INFO"
    assert "memory status=" in checks["capabilities"]["detail"]


def test_setup_check_reports_missing_dependencies() -> None:
    script = SKILLS / "resourcera" / "scripts" / "verify_setup.py"
    result = subprocess.run(
        [sys.executable, "-I", "-S", str(script), "--json"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 1, result.stdout + result.stderr
    report = json.loads(result.stdout)
    assert report["ok"] is False
    assert any(
        check["check"] == "imports" and check["status"] == "FAIL" for check in report["checks"]
    )
