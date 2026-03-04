"""Regression tests for one-PDF pilot harness artifact contract."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pension_data.ops.one_pdf_pilot import (
    OnePdfPilotInput,
    one_pdf_pilot_input_contract,
    resolve_one_pdf_pilot_input,
    resolve_one_pdf_pilot_runtime_options,
    run_one_pdf_pilot,
)


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


def test_one_pdf_input_contract_includes_required_path_env_and_metadata_fields() -> None:
    contract = one_pdf_pilot_input_contract()
    assert contract["required_input_fields"] == (
        "pdf_path",
        "plan_id",
        "plan_period",
        "effective_date",
        "ingestion_date",
    )
    assert contract["path_fields"] == ("pdf_path",)
    assert contract["optional_metadata_fields"] == (
        "source_url",
        "source_document_id",
        "fetched_at",
        "mime_type",
        "default_money_unit_scale",
    )

    env_var_by_field = contract["env_var_by_field"]
    assert isinstance(env_var_by_field, dict)
    assert env_var_by_field["pdf_path"] == "ONE_PDF_PILOT_PDF_PATH"
    assert env_var_by_field["plan_id"] == "ONE_PDF_PILOT_PLAN_ID"
    assert env_var_by_field["plan_period"] == "ONE_PDF_PILOT_PLAN_PERIOD"
    assert env_var_by_field["effective_date"] == "ONE_PDF_PILOT_EFFECTIVE_DATE"
    assert env_var_by_field["ingestion_date"] == "ONE_PDF_PILOT_INGESTION_DATE"
    assert env_var_by_field["source_url"] == "ONE_PDF_PILOT_SOURCE_URL"
    assert env_var_by_field["source_document_id"] == "ONE_PDF_PILOT_SOURCE_DOCUMENT_ID"
    assert env_var_by_field["fetched_at"] == "ONE_PDF_PILOT_FETCHED_AT"


def test_resolve_one_pdf_pilot_input_uses_env_fallback(tmp_path: Path) -> None:
    pdf_path = tmp_path / "pilot.pdf"
    _write_pdf_like_text(pdf_path, "Funded Ratio: 78.4%")
    env = {
        "ONE_PDF_PILOT_PDF_PATH": str(pdf_path),
        "ONE_PDF_PILOT_PLAN_ID": "CA-PERS",
        "ONE_PDF_PILOT_PLAN_PERIOD": "FY2024",
        "ONE_PDF_PILOT_EFFECTIVE_DATE": "2024-06-30",
        "ONE_PDF_PILOT_INGESTION_DATE": "2026-03-03",
        "ONE_PDF_PILOT_SOURCE_URL": "https://example.org/ca-2024.pdf",
        "ONE_PDF_PILOT_DEFAULT_MONEY_UNIT_SCALE": "million_usd",
    }
    resolved = resolve_one_pdf_pilot_input(
        pdf_path=None,
        plan_id=None,
        plan_period=None,
        effective_date=None,
        ingestion_date=None,
        env=env,
    )

    assert resolved.pdf_path == pdf_path
    assert resolved.plan_id == "CA-PERS"
    assert resolved.plan_period == "FY2024"
    assert resolved.source_url == "https://example.org/ca-2024.pdf"
    assert resolved.default_money_unit_scale == "million_usd"


def test_resolve_one_pdf_pilot_input_prefers_cli_values_over_env(tmp_path: Path) -> None:
    cli_pdf_path = tmp_path / "cli.pdf"
    env_pdf_path = tmp_path / "env.pdf"
    _write_pdf_like_text(cli_pdf_path, "Funded Ratio: 78.4%")
    _write_pdf_like_text(env_pdf_path, "Funded Ratio: 78.4%")
    env = {
        "ONE_PDF_PILOT_PDF_PATH": str(env_pdf_path),
        "ONE_PDF_PILOT_PLAN_ID": "ENV",
        "ONE_PDF_PILOT_PLAN_PERIOD": "ENV",
        "ONE_PDF_PILOT_EFFECTIVE_DATE": "2023-06-30",
        "ONE_PDF_PILOT_INGESTION_DATE": "2026-01-01",
        "ONE_PDF_PILOT_DEFAULT_MONEY_UNIT_SCALE": "usd",
    }
    resolved = resolve_one_pdf_pilot_input(
        pdf_path=cli_pdf_path,
        plan_id="CLI",
        plan_period="FY2024",
        effective_date="2024-06-30",
        ingestion_date="2026-03-03",
        default_money_unit_scale="thousand_usd",
        env=env,
    )

    assert resolved.pdf_path == cli_pdf_path
    assert resolved.plan_id == "CLI"
    assert resolved.plan_period == "FY2024"
    assert resolved.default_money_unit_scale == "thousand_usd"


def test_resolve_runtime_options_uses_contract_defaults_and_env() -> None:
    output_root, run_id = resolve_one_pdf_pilot_runtime_options(
        output_root=None,
        run_id=None,
        env={
            "ONE_PDF_PILOT_OUTPUT_ROOT": "pilot-outputs",
            "ONE_PDF_PILOT_RUN_ID": "pilot-run-id",
        },
    )

    assert output_root == Path("pilot-outputs")
    assert run_id == "pilot-run-id"
