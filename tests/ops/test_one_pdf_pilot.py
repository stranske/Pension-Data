"""Regression tests for one-PDF pilot harness artifact contract."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pension_data.ops.one_pdf_pilot import OnePdfPilotInput, run_one_pdf_pilot


def _write_pdf_like_text(path: Path, text: str) -> None:
    path.write_bytes(text.encode("latin-1"))


def test_one_pdf_pilot_writes_expected_artifact_contract(tmp_path: Path) -> None:
    pdf_path = tmp_path / "pilot.pdf"
    _write_pdf_like_text(
        pdf_path,
        "\n".join(
            (
                "Funded Ratio: 78.4%",
                "AAL: $640 million",
                "AVA: $501.8 million",
                "Discount Rate: 6.8%",
                "Employer Contribution Rate: 12.4%",
                "Employee Contribution Rate: 7.5%",
                "Participant Count: 325000",
            )
        ),
    )

    result = run_one_pdf_pilot(
        pilot_input=OnePdfPilotInput(
            pdf_path=pdf_path,
            plan_id="CA-PERS",
            plan_period="FY2024",
            effective_date="2024-06-30",
            ingestion_date="2026-03-03",
        ),
        output_root=tmp_path / "outputs",
        run_id="pilot-contract",
    )

    manifest_path = Path(result["run_manifest_json"])
    assert manifest_path.exists()
    parser_path = Path(result["parser_result_json"])
    coverage_path = Path(result["coverage_summary_json"])
    assert parser_path.exists()
    assert coverage_path.exists()
    assert Path(result["persistence_contract_json"]).exists()
    assert Path(result["staging_core_metrics_json"]).exists()
    assert Path(result["staging_manager_fund_vehicle_relationships_json"]).exists()
    assert Path(result["extraction_warnings_json"]).exists()
    assert Path(result["schema_component_datasets_manifest_json"]).exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    artifact_files = manifest["artifact_files"]
    expected_keys = {
        "parser_result_json",
        "coverage_summary_json",
        "persistence_contract_json",
        "staging_core_metrics_json",
        "staging_manager_fund_vehicle_relationships_json",
        "extraction_warnings_json",
        "schema_component_datasets_manifest_json",
        "orchestration_ledger_json",
        "orchestration_published_rows_json",
        "orchestration_review_queue_rows_json",
        "orchestration_state_json",
    }
    assert set(artifact_files) == expected_keys
    for artifact_path in artifact_files.values():
        assert Path(artifact_path).exists()

    coverage = json.loads(coverage_path.read_text(encoding="utf-8"))
    assert coverage["has_required_funded_metrics"] is True
    assert coverage["staging_core_metric_count"] > 0
    assert coverage["missing_required_metrics"] == []


def test_one_pdf_pilot_fails_when_required_metrics_are_missing(tmp_path: Path) -> None:
    pdf_path = tmp_path / "missing-metrics.pdf"
    _write_pdf_like_text(pdf_path, "This page does not include funded metric labels.")

    with pytest.raises(ValueError, match="Unable to parse required funded metrics"):
        run_one_pdf_pilot(
            pilot_input=OnePdfPilotInput(
                pdf_path=pdf_path,
                plan_id="CA-PERS",
                plan_period="FY2024",
                effective_date="2024-06-30",
                ingestion_date="2026-03-03",
            ),
            output_root=tmp_path / "outputs",
            run_id="pilot-missing-metrics",
        )


def test_one_pdf_pilot_default_source_document_id_is_content_stable(
    tmp_path: Path,
) -> None:
    payload = "\n".join(
        (
            "Funded Ratio: 78.4%",
            "AAL: $640 million",
            "AVA: $501.8 million",
            "Discount Rate: 6.8%",
            "Employer Contribution Rate: 12.4%",
            "Employee Contribution Rate: 7.5%",
            "Participant Count: 325000",
        )
    )
    first_pdf = tmp_path / "a" / "pilot.pdf"
    second_pdf = tmp_path / "b" / "renamed.pdf"
    first_pdf.parent.mkdir(parents=True, exist_ok=True)
    second_pdf.parent.mkdir(parents=True, exist_ok=True)
    _write_pdf_like_text(first_pdf, payload)
    _write_pdf_like_text(second_pdf, payload)

    first = run_one_pdf_pilot(
        pilot_input=OnePdfPilotInput(
            pdf_path=first_pdf,
            plan_id="CA-PERS",
            plan_period="FY2024",
            effective_date="2024-06-30",
            ingestion_date="2026-03-03",
        ),
        output_root=tmp_path / "outputs",
        run_id="pilot-first",
    )
    second = run_one_pdf_pilot(
        pilot_input=OnePdfPilotInput(
            pdf_path=second_pdf,
            plan_id="CA-PERS",
            plan_period="FY2024",
            effective_date="2024-06-30",
            ingestion_date="2026-03-03",
        ),
        output_root=tmp_path / "outputs",
        run_id="pilot-second",
    )

    first_manifest = json.loads(Path(first["run_manifest_json"]).read_text(encoding="utf-8"))
    second_manifest = json.loads(Path(second["run_manifest_json"]).read_text(encoding="utf-8"))

    assert (
        first_manifest["input"]["source_document_id"]
        == second_manifest["input"]["source_document_id"]
    )
