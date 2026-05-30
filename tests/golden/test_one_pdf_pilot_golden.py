"""Golden gate test: the committed pilot baseline tracks a real pilot run."""

from __future__ import annotations

import json
from pathlib import Path

from pension_data.ops.one_pdf_pilot import OnePdfPilotInput, run_one_pdf_pilot
from tools.ci_quality.manifest_gate import diff_manifest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_FIXTURE = _REPO_ROOT / "tests" / "golden" / "one_pdf_pilot" / "fixture_synthetic.pdf"
_BASELINE = _REPO_ROOT / "tests" / "golden" / "one_pdf_pilot" / "run_manifest_baseline.json"


def _run_pilot(tmp_path: Path) -> dict[str, object]:
    result = run_one_pdf_pilot(
        pilot_input=OnePdfPilotInput(
            pdf_path=_FIXTURE,
            plan_id="CA-PERS",
            plan_period="FY2024",
            effective_date="2024-06-30",
            ingestion_date="2026-03-03",
        ),
        output_root=tmp_path / "out",
        run_id="pilot-golden",
    )
    return json.loads(Path(result["run_manifest_json"]).read_text(encoding="utf-8"))


def test_committed_fixture_exists() -> None:
    assert _FIXTURE.exists(), "synthetic golden fixture must be committed"
    assert _BASELINE.exists(), "committed pilot baseline must exist"


def test_pilot_run_matches_committed_baseline(tmp_path: Path) -> None:
    manifest = _run_pilot(tmp_path)
    baseline = json.loads(_BASELINE.read_text(encoding="utf-8"))
    report = diff_manifest(baseline=baseline, current_manifest=manifest)
    assert report["unexpected_changes"] == 0, report


def test_perturbing_a_baseline_key_is_detected(tmp_path: Path) -> None:
    manifest = _run_pilot(tmp_path)
    baseline = json.loads(_BASELINE.read_text(encoding="utf-8"))
    baseline["artifact_files_keys"] = [
        key for key in baseline["artifact_files_keys"] if key != "parser_result_json"
    ] + ["unexpected_extra_json"]
    report = diff_manifest(baseline=baseline, current_manifest=manifest)
    assert report["unexpected_changes"] > 0


def test_perturbing_ledger_status_is_detected(tmp_path: Path) -> None:
    manifest = _run_pilot(tmp_path)
    baseline = json.loads(_BASELINE.read_text(encoding="utf-8"))
    baseline["ledger_status"] = "definitely-not-the-real-status"
    report = diff_manifest(baseline=baseline, current_manifest=manifest)
    assert any(change["field"] == "ledger_status" for change in report["changes"])
