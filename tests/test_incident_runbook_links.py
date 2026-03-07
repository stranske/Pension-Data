"""Tests for incident-to-runbook link mapping used in pipeline summaries."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HELPER_PATH = ROOT / ".github" / "scripts" / "incident_runbook_links.js"
KEEPALIVE_PATH = ROOT / ".github" / "scripts" / "keepalive_loop.js"


def _run_node(script: str) -> str:
    if shutil.which("node") is None:
        pytest.skip("node is required for JS helper tests")
    completed = subprocess.run(
        ["node", "-e", script],
        check=True,
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    return completed.stdout.strip()


@pytest.mark.parametrize(
    ("message", "expected_id", "expected_path"),
    [
        (
            "source_map_breakage with SCHEMA_VALIDATION failure in source-map lint",
            "source_map_breakage",
            "docs/runbooks/source-map-breakage.md#source-map-breakage",
        ),
        (
            "revised_file_mismatch due to supersession plan period drift",
            "revised_file_mismatch",
            "docs/runbooks/revised-file-mismatch.md#revised-file-mismatch",
        ),
        (
            "parser_fallback_exhaustion: all fallback stages exhausted required fields missing",
            "parser_fallback_exhaustion",
            "docs/runbooks/parser-fallback-exhaustion.md#parser-fallback-exhaustion",
        ),
        (
            "parser_output_validation_failure: schema_invalid and provenance_invalid blocked promotion",
            "parser_output_validation_failure",
            "docs/runbooks/parser-output-validation-failure.md#parser-output-validation-failure",
        ),
        (
            "parser_low_confidence_output routed to high-priority review queue",
            "parser_low_confidence_output",
            "docs/runbooks/parser-low-confidence-output.md#parser-low-confidence-output",
        ),
        (
            "anomaly_flood detected with anomaly spike and queue depth growth",
            "anomaly_flood",
            "docs/runbooks/anomaly-flood.md#anomaly-flood",
        ),
    ],
)
def test_detect_incident_runbooks(message: str, expected_id: str, expected_path: str) -> None:
    script = f"""
const {{ detectIncidentRunbooks }} = require("./.github/scripts/incident_runbook_links");
const result = detectIncidentRunbooks({json.dumps(message)});
console.log(JSON.stringify(result.map((entry) => [entry.id, entry.path])));
"""
    output = _run_node(script)
    result = json.loads(output)
    assert [expected_id, expected_path] in result


def test_build_incident_runbook_section_renders_markdown_lines() -> None:
    script = """
const { buildIncidentRunbookSection } = require("./.github/scripts/incident_runbook_links");
const lines = buildIncidentRunbookSection("parser_fallback_exhaustion during extraction");
console.log(JSON.stringify(lines));
"""
    output = _run_node(script)
    lines = json.loads(output)
    assert "### Incident Runbooks" in lines
    assert any(
        "parser_fallback_exhaustion" in line
        and "docs/runbooks/parser-fallback-exhaustion.md#parser-fallback-exhaustion" in line
        for line in lines
    )


def test_detect_incident_runbooks_avoids_over_broad_matches() -> None:
    script = """
const { detectIncidentRunbooks } = require("./.github/scripts/incident_runbook_links");
const result = detectIncidentRunbooks("source map referenced for docs index only");
console.log(JSON.stringify(result.map((entry) => entry.id)));
"""
    output = _run_node(script)
    assert json.loads(output) == []


def test_keepalive_loop_no_longer_wires_runbook_section_builder() -> None:
    text = KEEPALIVE_PATH.read_text(encoding="utf-8")
    assert "buildIncidentRunbookSection" not in text
    assert "const runbookSectionLines = buildIncidentRunbookSection" not in text
