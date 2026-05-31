"""End-to-end pilot coverage against the committed REAL CalPERS PDF fixture.

The synthetic golden gate (``tests/golden/test_one_pdf_pilot_golden.py``) exercises
``fixture_synthetic.pdf``; the real heuristic PDF parser path is otherwise only
covered by unit tests that synthesize ``*.pdf`` text in ``tmp_path``. This module
runs the production ``run_one_pdf_pilot`` harness against the committed real PDF
(``tests/parser/fixtures/calpers_fy2024_excerpt.pdf``) so a regression in the
table-primary parser on an actual document is caught. Deterministic: no network,
no LLM, no OCR (the fixture is covered by the ``table_primary`` stage).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pension_data.ops.one_pdf_pilot import OnePdfPilotInput, run_one_pdf_pilot

_REPO_ROOT = Path(__file__).resolve().parents[2]
_REAL_FIXTURE = _REPO_ROOT / "tests" / "parser" / "fixtures" / "calpers_fy2024_excerpt.pdf"


def _write_pdf_like_text(path: Path, text: str) -> None:
    path.write_bytes(text.encode("latin-1"))


def test_real_calpers_fixture_is_committed() -> None:
    assert _REAL_FIXTURE.exists(), "real CalPERS PDF fixture must stay committed"


def test_pilot_extracts_required_metrics_from_calpers_fixture(tmp_path: Path) -> None:
    result = run_one_pdf_pilot(
        pilot_input=OnePdfPilotInput(
            pdf_path=_REAL_FIXTURE,
            plan_id="CA-PERS",
            plan_period="FY2024",
            effective_date="2024-06-30",
            ingestion_date="2026-01-01",
        ),
        output_root=tmp_path / "out",
        run_id="real-pdf-pilot",
    )

    # The harness self-guards via a ValueError when required funded metrics are
    # missing, so a returned manifest already proves extraction succeeded; assert
    # the observable success signals explicitly.
    manifest = json.loads(Path(result["run_manifest_json"]).read_text(encoding="utf-8"))
    assert manifest["ledger_status"] == "success", manifest

    parser_result = json.loads(Path(result["parser_result_json"]).read_text(encoding="utf-8"))
    assert parser_result["missing_metrics"] == [], parser_result
    assert parser_result["stage_name"] == "table_primary", parser_result
    assert parser_result["escalation_required"] is False, parser_result

    # The pilot must materialize the named artifact-contract files.
    for key in (
        "parser_result_json",
        "staging_core_metrics_json",
        "coverage_summary_json",
        "run_manifest_json",
    ):
        assert Path(result[key]).exists(), f"missing emitted artifact: {key}"

    staging_core_metrics = json.loads(
        Path(result["staging_core_metrics_json"]).read_text(encoding="utf-8")
    )
    assert isinstance(staging_core_metrics, list)
    assert staging_core_metrics, "real-PDF pilot must persist at least one core metric row"


def test_pilot_raises_when_required_metrics_missing(tmp_path: Path) -> None:
    # Guards the assertion above: feeding a document with no funded-metric labels
    # must hit the "Unable to parse required funded metrics" path so the real-PDF
    # test cannot silently pass on an extraction regression.
    empty_pdf = tmp_path / "no-metrics.pdf"
    _write_pdf_like_text(empty_pdf, "This page does not include funded metric labels.")

    with pytest.raises(ValueError, match="Unable to parse required funded metrics"):
        run_one_pdf_pilot(
            pilot_input=OnePdfPilotInput(
                pdf_path=empty_pdf,
                plan_id="CA-PERS",
                plan_period="FY2024",
                effective_date="2024-06-30",
                ingestion_date="2026-01-01",
            ),
            output_root=tmp_path / "out-missing",
            run_id="real-pdf-pilot-missing",
        )
