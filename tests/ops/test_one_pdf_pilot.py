"""Regression tests for one-PDF pilot harness artifact contract."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pension_data.db.models.artifacts import RawArtifactRecord
from pension_data.ops.document_orchestration import DocumentOrchestrationState
from pension_data.ops.one_pdf_pilot import (
    OnePdfPilotInput,
    one_pdf_pilot_input_contract,
    resolve_one_pdf_pilot_input,
    resolve_one_pdf_pilot_runtime_options,
    run_one_pdf_pilot,
)
from pension_data.sources.schema import SOURCE_AUTHORITY_TIERS, SourceAuthorityTier


def _write_pdf_like_text(path: Path, text: str) -> None:
    path.write_bytes(text.encode("latin-1"))


@pytest.mark.parametrize("authority_tier", [None, *SOURCE_AUTHORITY_TIERS])
def test_one_pdf_pilot_writes_expected_artifact_contract(
    tmp_path: Path, authority_tier: SourceAuthorityTier | None
) -> None:
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
            source_authority_tier=authority_tier,
        ),
        output_root=tmp_path / "outputs",
        run_id="pilot-contract",
    )

    manifest_path = Path(result["run_manifest_json"])
    assert manifest_path.exists()
    parser_path = Path(result["parser_result_json"])
    coverage_path = Path(result["coverage_summary_json"])
    component_report_path = Path(result["component_coverage_report_json"])
    assert parser_path.exists()
    assert coverage_path.exists()
    assert component_report_path.exists()
    assert Path(result["persistence_contract_json"]).exists()
    assert Path(result["staging_core_metrics_json"]).exists()
    assert Path(result["staging_manager_fund_vehicle_relationships_json"]).exists()
    assert Path(result["extraction_warnings_json"]).exists()
    assert Path(result["schema_component_datasets_manifest_json"]).exists()
    assert Path(result["workspace_bundle_json"]).exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    parser_payload = json.loads(parser_path.read_text(encoding="utf-8"))
    expected_context = {
        "actionable_flags": parser_payload["actionable_flags"],
        "escalation_required": parser_payload["escalation_required"],
        "source_authority_status": "declared" if authority_tier is not None else "unknown",
        "source_authority_tier": authority_tier,
    }
    assert manifest["input"]["source_authority_tier"] == authority_tier
    assert manifest["review_context"] == expected_context
    bundle = json.loads(Path(result["workspace_bundle_json"]).read_text(encoding="utf-8"))
    assert bundle["datasets"][0]["rows"]
    for row in bundle["datasets"][0]["rows"]:
        assert row["review_context"] == expected_context
    artifact_files = manifest["artifact_files"]
    expected_keys = {
        "parser_result_json",
        "coverage_summary_json",
        "component_coverage_report_json",
        "persistence_contract_json",
        "staging_core_metrics_json",
        "staging_manager_fund_vehicle_relationships_json",
        "extraction_warnings_json",
        "schema_component_datasets_manifest_json",
        "orchestration_ledger_json",
        "orchestration_published_rows_json",
        "orchestration_review_queue_rows_json",
        "orchestration_state_json",
        "workspace_bundle_json",
    }
    expected_result_keys = expected_keys | {"run_id", "run_manifest_json"}
    expected_keys.add("financial_flow_rows_json")
    assert set(artifact_files) == expected_keys
    assert set(result) == expected_result_keys
    for artifact_path in artifact_files.values():
        assert Path(artifact_path).exists()

    coverage = json.loads(coverage_path.read_text(encoding="utf-8"))
    assert coverage["has_required_funded_metrics"] is True
    assert coverage["staging_core_metric_count"] > 0
    assert coverage["missing_required_metrics"] == []
    component_report = json.loads(component_report_path.read_text(encoding="utf-8"))
    assert component_report["is_valid"] is True
    assert component_report["expected_component_count"] == 19


def test_one_pdf_pilot_artifacts_follow_deterministic_layout(tmp_path: Path) -> None:
    pdf_path = tmp_path / "layout.pdf"
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

    output_root = tmp_path / "artifacts"
    run_id = "pilot-layout"
    result = run_one_pdf_pilot(
        pilot_input=OnePdfPilotInput(
            pdf_path=pdf_path,
            plan_id="CA-PERS",
            plan_period="FY2024",
            effective_date="2024-06-30",
            ingestion_date="2026-03-03",
        ),
        output_root=output_root,
        run_id=run_id,
    )

    expected_run_root = output_root / "one_pdf_pilot" / run_id
    assert Path(result["run_manifest_json"]) == expected_run_root / "run_manifest.json"
    assert Path(result["parser_result_json"]) == expected_run_root / "parser_result.json"
    assert Path(result["workspace_bundle_json"]) == expected_run_root / "workspace-bundle.json"
    assert Path(result["coverage_summary_json"]) == (
        expected_run_root / "coverage" / "component_coverage_summary.json"
    )
    assert Path(result["component_coverage_report_json"]) == (
        expected_run_root / "coverage" / "component_coverage_report.json"
    )
    assert Path(result["persistence_contract_json"]) == (
        expected_run_root / "extraction_persistence" / "persistence_contract.json"
    )
    assert Path(result["staging_core_metrics_json"]) == (
        expected_run_root / "extraction_persistence" / "staging_core_metrics.json"
    )
    assert Path(result["staging_manager_fund_vehicle_relationships_json"]) == (
        expected_run_root
        / "extraction_persistence"
        / "staging_manager_fund_vehicle_relationships.json"
    )
    assert Path(result["extraction_warnings_json"]) == (
        expected_run_root / "extraction_persistence" / "extraction_warnings.json"
    )
    assert Path(result["schema_component_datasets_manifest_json"]) == (
        expected_run_root / "extraction_persistence" / "component_datasets_manifest.json"
    )

    manifest = json.loads(Path(result["run_manifest_json"]).read_text(encoding="utf-8"))
    artifact_paths = {key: Path(value) for key, value in manifest["artifact_files"].items()}
    assert artifact_paths["orchestration_ledger_json"] == (
        expected_run_root / "document_orchestration" / run_id / "ledger.json"
    )
    assert artifact_paths["orchestration_published_rows_json"] == (
        expected_run_root / "document_orchestration" / run_id / "published_rows.json"
    )
    assert artifact_paths["orchestration_review_queue_rows_json"] == (
        expected_run_root / "document_orchestration" / run_id / "review_queue_rows.json"
    )
    assert artifact_paths["orchestration_state_json"] == (
        expected_run_root / "document_orchestration" / run_id / "state.json"
    )
    for artifact_path in artifact_paths.values():
        assert artifact_path.is_relative_to(expected_run_root)


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


def test_one_pdf_pilot_default_run_id_is_input_stable(tmp_path: Path) -> None:
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

    first = run_one_pdf_pilot(
        pilot_input=OnePdfPilotInput(
            pdf_path=pdf_path,
            plan_id="CA-PERS",
            plan_period="FY2024",
            effective_date="2024-06-30",
            ingestion_date="2026-03-03",
        ),
        output_root=tmp_path / "outputs",
    )
    second = run_one_pdf_pilot(
        pilot_input=OnePdfPilotInput(
            pdf_path=pdf_path,
            plan_id="CA-PERS",
            plan_period="FY2024",
            effective_date="2024-06-30",
            ingestion_date="2026-03-03",
        ),
        output_root=tmp_path / "outputs",
    )

    assert first["run_id"] == second["run_id"]
    assert first["run_manifest_json"] == second["run_manifest_json"]


def test_one_pdf_pilot_skipped_rerun_reuses_persisted_workspace_rows(tmp_path: Path) -> None:
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
    pilot_input = OnePdfPilotInput(
        pdf_path=pdf_path,
        plan_id="CA-PERS",
        plan_period="FY2024",
        effective_date="2024-06-30",
        ingestion_date="2026-03-03",
    )
    output_root = tmp_path / "outputs"

    first = run_one_pdf_pilot(
        pilot_input=pilot_input,
        output_root=output_root,
        run_id="pilot-skipped-rerun",
    )
    prior_state_payload = json.loads(
        Path(first["orchestration_state_json"]).read_text(encoding="utf-8")
    )
    prior_state = DocumentOrchestrationState(
        artifact_records=tuple(
            RawArtifactRecord(**row) for row in prior_state_payload["artifact_records"]
        ),
        processed_artifact_ids=tuple(prior_state_payload["processed_artifact_ids"]),
        published_fact_ids=tuple(prior_state_payload["published_fact_ids"]),
    )

    second = run_one_pdf_pilot(
        pilot_input=pilot_input,
        output_root=output_root,
        run_id="pilot-skipped-rerun",
        state=prior_state,
    )

    first_bundle = json.loads(Path(first["workspace_bundle_json"]).read_text(encoding="utf-8"))
    second_bundle = json.loads(Path(second["workspace_bundle_json"]).read_text(encoding="utf-8"))
    assert first_bundle["datasets"]
    assert second_bundle == first_bundle


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
        "source_authority_tier",
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
    assert env_var_by_field["source_authority_tier"] == "ONE_PDF_PILOT_SOURCE_AUTHORITY_TIER"


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
        "ONE_PDF_PILOT_SOURCE_AUTHORITY_TIER": " official-mirror ",
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
    assert resolved.source_authority_tier == "official-mirror"


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
        "ONE_PDF_PILOT_SOURCE_AUTHORITY_TIER": "official",
    }
    resolved = resolve_one_pdf_pilot_input(
        pdf_path=cli_pdf_path,
        plan_id="CLI",
        plan_period="FY2024",
        effective_date="2024-06-30",
        ingestion_date="2026-03-03",
        default_money_unit_scale="thousand_usd",
        source_authority_tier=" high-confidence-third-party ",
        env=env,
    )

    assert resolved.pdf_path == cli_pdf_path
    assert resolved.plan_id == "CLI"
    assert resolved.plan_period == "FY2024"
    assert resolved.default_money_unit_scale == "thousand_usd"
    assert resolved.source_authority_tier == "high-confidence-third-party"


@pytest.mark.parametrize(
    "authority_tier", ["unverified-blog", "OFFICIAL", "", " ", False, 1, [], {}]
)
def test_one_pdf_input_rejects_invalid_declared_authority(
    tmp_path: Path, authority_tier: object
) -> None:
    inputs = {
        "pdf_path": tmp_path / "pilot.pdf",
        "plan_id": "CA-PERS",
        "plan_period": "FY2024",
        "effective_date": "2024-06-30",
        "ingestion_date": "2026-03-03",
    }
    with pytest.raises(ValueError, match="source_authority_tier"):
        OnePdfPilotInput(**inputs, source_authority_tier=authority_tier)

    # An explicitly invalid value must not silently fall back to valid environment metadata.
    with pytest.raises(ValueError, match="source_authority_tier"):
        resolve_one_pdf_pilot_input(
            **inputs,
            source_authority_tier=authority_tier,
            env={"ONE_PDF_PILOT_SOURCE_AUTHORITY_TIER": "official"},
        )


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


def test_skipped_pilot_replay_preserves_disclosed_financial_flows(tmp_path, monkeypatch):
    from dataclasses import replace

    import pension_data.ops.one_pdf_pilot as pilot_module
    from pension_data.api.artifact_data import load_query_data_snapshot
    from pension_data.query.saved_views.service import execute_funding_trend_view

    original_parser = pilot_module.parse_pdf_to_funded_input

    def disclosed_parser(payload):
        result = original_parser(payload)
        assert result.raw is not None
        flow_rows = tuple(
            {"label": label, "value": value, "evidence_ref": "p.1#flows"}
            for label, value in [
                ("Employer Contributions", "$100 million"),
                ("Employee Contributions", "$20 million"),
                ("Benefit Payments", "$-30 million"),
                ("Refunds", "$-5 million"),
            ]
        )
        return replace(
            result, raw=replace(result.raw, table_rows=(*result.raw.table_rows, *flow_rows))
        )

    monkeypatch.setattr(pilot_module, "parse_pdf_to_funded_input", disclosed_parser)
    fixture = Path(__file__).resolve().parents[1] / "golden/one_pdf_pilot/fixture_synthetic.pdf"
    pilot_input = OnePdfPilotInput(
        pdf_path=fixture,
        plan_id="FLOW",
        plan_period="FY2024",
        effective_date="2024-06-30",
        ingestion_date="2026-01-01",
    )
    root = tmp_path / "outputs"
    first = run_one_pdf_pilot(pilot_input=pilot_input, output_root=root, run_id="replay-flows")
    manifest = json.loads(Path(first["run_manifest_json"]).read_text())
    flow_path = Path(manifest["artifact_files"]["financial_flow_rows_json"])
    before = json.loads(flow_path.read_text())
    assert len(before) == 1 and before[0]["net_external_cash_flow_usd"] == 85_000_000
    state_payload = json.loads(Path(first["orchestration_state_json"]).read_text())
    state = DocumentOrchestrationState(
        artifact_records=tuple(
            RawArtifactRecord(**row) for row in state_payload["artifact_records"]
        ),
        processed_artifact_ids=tuple(state_payload["processed_artifact_ids"]),
        published_fact_ids=tuple(state_payload["published_fact_ids"]),
    )
    second = run_one_pdf_pilot(
        pilot_input=pilot_input, output_root=root, run_id="replay-flows", state=state
    )
    ledger = json.loads(Path(second["orchestration_ledger_json"]).read_text())
    assert any(row["status"] == "skipped" for row in ledger["document_outcomes"])
    assert json.loads(flow_path.read_text()) == before
    trend = execute_funding_trend_view(list(load_query_data_snapshot(root).funding_trend_inputs))
    assert len(trend) == 1 and trend[0].net_external_cash_flow_usd == 85_000_000


def test_escalation_and_declared_authority_survive_to_bundle_and_api(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import importlib.util
    from dataclasses import replace

    from fastapi.testclient import TestClient

    import pension_data.ops.one_pdf_pilot as pilot_module
    from pension_data.api.app import create_app
    from pension_data.api.artifact_data import load_query_data_snapshot
    from pension_data.api.auth import SCOPE_QUERY, APIKeyStore

    builder_path = (
        Path(__file__).resolve().parents[2] / "scripts" / "web" / "build_workspace_bundle.py"
    )
    builder_spec = importlib.util.spec_from_file_location("build_workspace_bundle", builder_path)
    assert builder_spec is not None and builder_spec.loader is not None
    bundle_builder = importlib.util.module_from_spec(builder_spec)
    builder_spec.loader.exec_module(bundle_builder)

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

    original_parser = pilot_module.parse_pdf_to_funded_input
    parser_overrides: dict[str, tuple[bool, tuple[str, ...]]] = {
        "doc:ca-pers:fy2024:unknown-auth": (
            True,
            ("doc_lineage_unreadable_pages:1", "manual_review_needed"),
        ),
        "doc:ca-pers:fy2025:third-party": (
            True,
            ("low_confidence_stage:table_assist",),
        ),
        "doc:ca-pers:fy2023:legacy": (False, ()),
    }

    def escalated_parser(payload):
        result = original_parser(payload)
        escalation_required, flags = parser_overrides[payload.source_document_id]
        return replace(
            result,
            escalation_required=escalation_required,
            actionable_flags=flags,
        )

    monkeypatch.setattr(pilot_module, "parse_pdf_to_funded_input", escalated_parser)
    output_root = tmp_path / "outputs"

    # 1. Simultaneous parser escalation + unresolved/unknown authority (HTTPS URL must not infer authority).
    run_unknown = run_one_pdf_pilot(
        pilot_input=OnePdfPilotInput(
            pdf_path=pdf_path,
            plan_id="CA-PERS",
            plan_period="FY2024",
            effective_date="2024-06-30",
            ingestion_date="2026-03-03",
            source_url="https://www.calpers.ca.gov/docs/fy2024.pdf",
            source_document_id="doc:ca-pers:fy2024:unknown-auth",
        ),
        output_root=output_root,
        run_id="run-escalated-unknown-authority",
    )
    expected_unknown_context = {
        "actionable_flags": ["doc_lineage_unreadable_pages:1", "manual_review_needed"],
        "escalation_required": True,
        "source_authority_status": "unknown",
        "source_authority_tier": None,
    }
    manifest_unknown = json.loads(
        Path(run_unknown["run_manifest_json"]).read_text(encoding="utf-8")
    )
    assert manifest_unknown["input"].get("source_authority_tier") is None
    assert manifest_unknown.get("review_context") == expected_unknown_context

    direct_unknown_bundle = json.loads(
        Path(run_unknown["workspace_bundle_json"]).read_text(encoding="utf-8")
    )
    rebuilt_unknown_bundle = bundle_builder.build_workspace_bundle(
        Path(run_unknown["run_manifest_json"]).parent
    )
    assert (
        [row.get("review_context") for row in direct_unknown_bundle["datasets"][0]["rows"]],
        [row.get("review_context") for row in rebuilt_unknown_bundle["datasets"][0]["rows"]],
    ) == (
        [expected_unknown_context] * len(direct_unknown_bundle["datasets"][0]["rows"]),
        [expected_unknown_context] * len(rebuilt_unknown_bundle["datasets"][0]["rows"]),
    )

    with pytest.raises(ValueError, match="source_authority_tier must be one of"):
        OnePdfPilotInput(
            pdf_path=pdf_path,
            plan_id="CA-PERS",
            plan_period="FY2024",
            effective_date="2024-06-30",
            ingestion_date="2026-03-03",
            source_authority_tier="unverified-blog",  # type: ignore[arg-type]
        )

    with pytest.raises(ValueError, match="source_authority_tier must be one of"):
        resolve_one_pdf_pilot_input(
            pdf_path=pdf_path,
            plan_id="CA-PERS",
            plan_period="FY2024",
            effective_date="2024-06-30",
            ingestion_date="2026-03-03",
            env={"ONE_PDF_PILOT_SOURCE_AUTHORITY_TIER": "unofficial"},
        )

    # 2. Simultaneous parser escalation + declared source authority tier.
    run_declared = run_one_pdf_pilot(
        pilot_input=OnePdfPilotInput(
            pdf_path=pdf_path,
            plan_id="CA-PERS",
            plan_period="FY2025",
            effective_date="2025-06-30",
            ingestion_date="2026-03-04",
            source_document_id="doc:ca-pers:fy2025:third-party",
            source_authority_tier="high-confidence-third-party",
        ),
        output_root=output_root,
        run_id="run-escalated-declared-authority",
    )
    expected_declared_context = {
        "actionable_flags": ["low_confidence_stage:table_assist"],
        "escalation_required": True,
        "source_authority_status": "declared",
        "source_authority_tier": "high-confidence-third-party",
    }
    manifest_declared = json.loads(
        Path(run_declared["run_manifest_json"]).read_text(encoding="utf-8")
    )
    assert manifest_declared["input"]["source_authority_tier"] == "high-confidence-third-party"
    assert manifest_declared["review_context"] == expected_declared_context

    direct_declared_bundle = json.loads(
        Path(run_declared["workspace_bundle_json"]).read_text(encoding="utf-8")
    )
    rebuilt_declared_bundle = bundle_builder.build_workspace_bundle(
        Path(run_declared["run_manifest_json"]).parent
    )
    assert (
        [row["review_context"] for row in direct_declared_bundle["datasets"][0]["rows"]],
        [row["review_context"] for row in rebuilt_declared_bundle["datasets"][0]["rows"]],
    ) == (
        [expected_declared_context] * len(direct_declared_bundle["datasets"][0]["rows"]),
        [expected_declared_context] * len(rebuilt_declared_bundle["datasets"][0]["rows"]),
    )

    # 3. Legacy artifact without persisted review_context or declared authority gets explicit unknowns.
    run_legacy = run_one_pdf_pilot(
        pilot_input=OnePdfPilotInput(
            pdf_path=pdf_path,
            plan_id="CA-PERS",
            plan_period="FY2023",
            effective_date="2023-06-30",
            ingestion_date="2026-03-02",
            source_document_id="doc:ca-pers:fy2023:legacy",
        ),
        output_root=output_root,
        run_id="run-legacy-unknowns",
    )
    legacy_manifest_path = Path(run_legacy["run_manifest_json"])
    legacy_manifest = json.loads(legacy_manifest_path.read_text(encoding="utf-8"))
    legacy_manifest.pop("review_context", None)
    legacy_manifest["input"].pop("source_authority_tier", None)
    legacy_manifest_path.write_text(json.dumps(legacy_manifest, indent=2), encoding="utf-8")

    expected_legacy_context = {
        "actionable_flags": [],
        "escalation_required": None,
        "source_authority_status": "unknown",
        "source_authority_tier": None,
    }
    rebuilt_legacy_bundle = bundle_builder.build_workspace_bundle(legacy_manifest_path.parent)
    assert [row["review_context"] for row in rebuilt_legacy_bundle["datasets"][0]["rows"]] == [
        expected_legacy_context
    ] * len(rebuilt_legacy_bundle["datasets"][0]["rows"])

    # 4. Multi-run API snapshot and HTTP endpoint preserve each run/document's review_context.
    snapshot = load_query_data_snapshot(output_root)
    expected_by_doc = {
        "doc:ca-pers:fy2024:unknown-auth": expected_unknown_context,
        "doc:ca-pers:fy2025:third-party": expected_declared_context,
        "doc:ca-pers:fy2023:legacy": expected_legacy_context,
    }
    observed_snapshot_contexts = {
        row.source_document_id: row.review_context.to_dict() for row in snapshot.metric_history_rows
    }
    assert observed_snapshot_contexts == expected_by_doc
    for row in snapshot.metric_history_rows:
        assert row.review_context.to_dict() == expected_by_doc[row.source_document_id]

    monkeypatch.setenv("PENSION_DATA_DATA_ZONE", "proprietary")
    monkeypatch.setenv("PENSION_DATA_QUERY_ARTIFACT_ROOT", str(output_root))
    store = APIKeyStore()
    secret, _ = store.create_key(scopes=(SCOPE_QUERY,), label="pilot-review-context")
    client = TestClient(create_app(key_store=store))
    response = client.get(
        "/api/metric-history/CA-PERS",
        headers={"Authorization": f"Bearer {secret}"},
    )
    assert response.status_code == 200
    api_rows = response.json()["rows"]
    assert {row["source_document_id"] for row in api_rows} == set(expected_by_doc)
    for row in api_rows:
        assert row["review_context"] == expected_by_doc[row["source_document_id"]]
