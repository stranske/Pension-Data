"""Guardrails to keep operator runbooks present and actionable."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
INCIDENT_CLASSES_DOC = ROOT / "docs" / "ops" / "INCIDENT_CLASSES.md"
PIPELINE_LINKS_DOC = ROOT / "docs" / "runbooks" / "PIPELINE_RUNBOOK_LINKS.md"

RUNBOOKS: dict[str, str] = {
    "source_map_breakage": "source-map-breakage.md",
    "revised_file_mismatch": "revised-file-mismatch.md",
    "parser_fallback_exhaustion": "parser-fallback-exhaustion.md",
    "anomaly_flood": "anomaly-flood.md",
}


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_incident_class_index_exists_and_lists_all_classes() -> None:
    assert INCIDENT_CLASSES_DOC.exists(), "missing incident classes doc: INCIDENT_CLASSES.md"
    text = _read(INCIDENT_CLASSES_DOC)
    for incident_class, runbook in RUNBOOKS.items():
        assert f"`{incident_class}`" in text
        assert runbook in text


def test_incident_class_index_lists_exact_canonical_set() -> None:
    text = _read(INCIDENT_CLASSES_DOC)
    class_ids = set(re.findall(r"`([a-z0-9_]+)`", text))
    assert class_ids == set(RUNBOOKS)


def test_runbooks_exist_and_are_nonempty() -> None:
    for incident_class, filename in RUNBOOKS.items():
        runbook_path = ROOT / "docs" / "runbooks" / filename
        assert runbook_path.exists(), f"missing runbook for {incident_class}"
        text = _read(runbook_path)
        assert "Last reviewed:" in text
        assert "Incident class:" in text
        assert "## Diagnostic Commands" in text
        assert "## Remediation Steps" in text
        assert "TBD" not in text


def test_runbooks_define_numbered_remediation_sequences() -> None:
    for incident_class, filename in RUNBOOKS.items():
        runbook_path = ROOT / "docs" / "runbooks" / filename
        text = _read(runbook_path)
        section_match = re.search(
            r"## Remediation Steps\s*\n(.*?)(?:\n## |\Z)",
            text,
            flags=re.DOTALL,
        )
        assert section_match, f"missing remediation section in {incident_class}"
        steps = re.findall(r"^\d+\. ", section_match.group(1), flags=re.MULTILINE)
        assert (
            len(steps) >= 5
        ), f"remediation in {incident_class} must contain at least 5 ordered steps"


def test_pipeline_links_cover_all_incident_classes() -> None:
    assert (
        PIPELINE_LINKS_DOC.exists()
    ), "missing pipeline runbook links doc: PIPELINE_RUNBOOK_LINKS.md"
    text = _read(PIPELINE_LINKS_DOC)
    for incident_class, filename in RUNBOOKS.items():
        assert incident_class in text
        assert filename in text
