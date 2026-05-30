"""Contract tests for the first static UI/LangChain review artifact."""

from __future__ import annotations

import json
import tempfile
from dataclasses import fields
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from scripts.langchain import build_reviewable_findings_artifact as artifact_cli

from pension_data.langchain.findings_compare import CompareMetadata
from pension_data.langchain.findings_explain import ExplainMetadata
from pension_data.langchain.findings_export import FindingsExportArtifact
from pension_data.langchain.review_artifact import (
    REVIEWABLE_FINDINGS_ARTIFACT_PATH,
    REVIEWABLE_FINDINGS_SCHEMA_PATH,
    ReviewableFindingsArtifactError,
    build_extraction_quality_dashboard_artifact,
    minimal_findings_json_schema,
    reviewable_findings_schema,
    validate_reviewable_findings_artifact,
    write_reviewable_findings_artifact,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def _valid_artifact() -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        contract_path, readiness_path = _write_real_sources(Path(tmp_dir))
        return build_extraction_quality_dashboard_artifact(
            generated_at="2026-05-10T03:20:00Z",
            artifact_date="2026-05-10",
            persistence_contract_path=contract_path,
            readiness_csv_path=readiness_path,
        )


def test_schema_file_matches_python_contract() -> None:
    schema_path = REPO_ROOT / REVIEWABLE_FINDINGS_SCHEMA_PATH
    schema = json.loads(schema_path.read_text())

    assert schema == reviewable_findings_schema()
    assert schema["artifact_path"] == REVIEWABLE_FINDINGS_ARTIFACT_PATH
    assert schema["slice"]["first_slice"] == "extraction_quality_dashboard"
    assert "confidence" in schema["findings"]["required_filter_fields"]
    assert schema["minimal_findings_json_schema"] == minimal_findings_json_schema()


def test_minimal_findings_schema_includes_ui_and_explainer_required_fields() -> None:
    minimal_schema = minimal_findings_json_schema()
    required_fields = set(minimal_schema["required_fields"])
    assert {
        "finding_id",
        "entity",
        "period",
        "metric_family",
        "confidence",
        "provenance_refs",
        "citations",
    } <= required_fields


def test_cli_defaults_to_the_selected_first_artifact_slice() -> None:
    with patch("sys.argv", ["build_reviewable_findings_artifact.py"]):
        args = artifact_cli.parse_args()
    assert args.slice == "extraction_quality_dashboard"


def test_cli_rejects_unknown_slice_value() -> None:
    with (
        pytest.raises(SystemExit),
        patch(
            "sys.argv",
            ["build_reviewable_findings_artifact.py", "--slice", "funding_trend"],
        ),
    ):
        artifact_cli.parse_args()


def test_langchain_required_output_fields_are_non_empty_and_exposed() -> None:
    required_output_fields = reviewable_findings_schema()["langchain_actions"][
        "required_output_fields"
    ]
    assert required_output_fields
    assert set(required_output_fields) == {
        "request_id",
        "generated_at",
        "summary",
        "citations",
        "artifact_path",
    }

    metadata_fields = {field.name for field in fields(ExplainMetadata)} | {
        field.name for field in fields(CompareMetadata)
    }
    export_fields = {field.name for field in fields(FindingsExportArtifact)}
    exposed_fields = metadata_fields | export_fields | {"summary", "citations"}

    assert set(required_output_fields) <= exposed_fields


@pytest.mark.parametrize(
    "recorded_output_path",
    [
        REPO_ROOT / "tests/langchain/recorded_outputs/findings_funded_ratio_explain.json",
        REPO_ROOT / "tests/langchain/recorded_outputs/findings_period_compare.json",
    ],
)
def test_langchain_required_output_fields_are_non_empty_in_recorded_outputs(
    recorded_output_path: Path,
) -> None:
    required_output_fields = reviewable_findings_schema()["langchain_actions"][
        "required_output_fields"
    ]
    payload = json.loads(recorded_output_path.read_text(encoding="utf-8"))

    for field in required_output_fields:
        assert field in payload
        value = payload[field]
        if field == "citations":
            assert isinstance(value, list)
            assert value
            assert all(isinstance(citation, str) and citation.strip() for citation in value)
            continue
        assert isinstance(value, str)
        assert value.strip()


def test_valid_artifact_includes_static_ui_and_langchain_contract_fields() -> None:
    artifact = _valid_artifact()

    validate_reviewable_findings_artifact(artifact)


@pytest.mark.parametrize(
    ("field", "message"),
    [
        ("source_artifact_ids", "source_artifact_ids"),
        ("findings", "findings"),
        ("langchain_actions", "langchain_actions"),
    ],
)
def test_artifact_requires_publishing_and_interaction_fields(field: str, message: str) -> None:
    artifact = _valid_artifact()
    artifact.pop(field)

    with pytest.raises(ReviewableFindingsArtifactError, match=message):
        validate_reviewable_findings_artifact(artifact)


def test_artifact_rejects_uncited_or_low_quality_finding_rows() -> None:
    artifact = _valid_artifact()
    artifact["findings"][0]["citations"] = []

    with pytest.raises(ReviewableFindingsArtifactError, match="citations"):
        validate_reviewable_findings_artifact(artifact)

    artifact = _valid_artifact()
    artifact["findings"][0]["confidence"] = 1.2

    with pytest.raises(ReviewableFindingsArtifactError, match="confidence"):
        validate_reviewable_findings_artifact(artifact)


def test_artifact_rejects_wrong_slice_id_for_published_path() -> None:
    artifact = _valid_artifact()
    artifact["slice"]["slice_id"] = "allocation_peer_compare"

    with pytest.raises(ReviewableFindingsArtifactError, match="slice_id"):
        validate_reviewable_findings_artifact(artifact)


def test_artifact_rejects_boolean_confidence_values() -> None:
    artifact = _valid_artifact()
    artifact["findings"][0]["confidence"] = True

    with pytest.raises(ReviewableFindingsArtifactError, match="confidence"):
        validate_reviewable_findings_artifact(artifact)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda action: action.pop("question"),
            "question",
        ),
        (
            lambda action: action.__setitem__("question", ""),
            "question",
        ),
        (
            lambda action: action.pop("finding_ids"),
            "finding_ids",
        ),
        (
            lambda action: action.__setitem__("finding_ids", []),
            "finding_ids",
        ),
        (
            lambda action: action.__setitem__("finding_ids", ["finding:missing"]),
            "unknown findings",
        ),
    ],
)
def test_artifact_rejects_malformed_langchain_actions(mutation: Any, message: str) -> None:
    artifact = _valid_artifact()
    mutation(artifact["langchain_actions"][0])

    with pytest.raises(ReviewableFindingsArtifactError, match=message):
        validate_reviewable_findings_artifact(artifact)


def test_docs_pin_the_published_artifact_path_and_contract() -> None:
    docs = [
        REPO_ROOT / "docs/UI_LANGCHAIN_OPTIONS.md",
        REPO_ROOT / "docs/LANGCHAIN_FOUNDATIONS.md",
        REPO_ROOT / "docs/contracts/reviewable-findings-artifact-contract.md",
        REPO_ROOT / "docs/data/reviewable-findings/README.md",
    ]
    for path in docs:
        text = path.read_text()
        assert REVIEWABLE_FINDINGS_ARTIFACT_PATH in text
        assert "extraction_quality_dashboard" in text


def test_published_artifact_path_contains_valid_contract_payload() -> None:
    artifact_path = REPO_ROOT / REVIEWABLE_FINDINGS_ARTIFACT_PATH
    artifact = json.loads(artifact_path.read_text())

    validate_reviewable_findings_artifact(artifact)


def test_contract_audit_followup_dispositions_include_github_issue_links() -> None:
    audit_path = REPO_ROOT / "docs/reports/reviewable-findings-contract-audit.md"
    lines = audit_path.read_text(encoding="utf-8").splitlines()
    needs_followup_lines = [line for line in lines if "needs-follow-up-issue" in line]

    for line in needs_followup_lines:
        assert "github.com/" in line, line


def test_generator_includes_required_acceptance_fields() -> None:
    artifact = _valid_artifact()

    finding = artifact["findings"][0]
    assert finding["entity"]
    assert finding["period"]
    assert finding["metric_family"]
    assert finding["confidence"] >= 0
    assert finding["provenance_refs"]


def test_writer_persists_valid_artifact_json(tmp_path: Path) -> None:
    artifact = _valid_artifact()
    output = tmp_path / "reviewable-findings.json"

    written = write_reviewable_findings_artifact(artifact, output_path=output)

    assert written == output
    persisted = json.loads(output.read_text(encoding="utf-8"))
    validate_reviewable_findings_artifact(persisted)


_FIXTURE_FINDING_IDS = frozenset(
    {
        "finding:ca-pers:fy2024:funded-ratio",
        "finding:ca-pers:fy2023:funded-ratio",
    }
)


def _write_real_sources(tmp_path: Path) -> tuple[Path, Path]:
    contract_path = tmp_path / "persistence_contract.json"
    contract_path.write_text(
        json.dumps(
            {
                "staging_core_metrics": ["plan_id", "plan_period", "metric_name"],
                "extraction_warnings": ["plan_id", "plan_period", "warning_code"],
            }
        ),
        encoding="utf-8",
    )
    readiness_path = tmp_path / "source_authority_readiness.csv"
    readiness_path.write_text(
        "plan_id,plan_period,cohort,system_type,official_resolution_state,"
        "source_authority_tier,mismatch_reason,extraction_blocker_reason,"
        "is_extraction_ready,readiness_state\n"
        "OR-PERS,FY2024,west,public,resolved,tier_1,,,true,ready\n"
        "NY-CTRS,FY2024,east,public,unresolved,tier_2,missing_acfr,"
        "annual_report_unavailable,false,blocked\n",
        encoding="utf-8",
    )
    return contract_path, readiness_path


def test_build_without_source_paths_raises_before_fixture_fallback() -> None:
    with pytest.raises(ReviewableFindingsArtifactError, match="must be provided"):
        build_extraction_quality_dashboard_artifact()


def test_build_from_sources_derives_findings_from_real_data(tmp_path: Path) -> None:
    contract_path, readiness_path = _write_real_sources(tmp_path)

    artifact = build_extraction_quality_dashboard_artifact(
        generated_at="2026-05-13T20:00:00Z",
        artifact_date="2026-05-13",
        persistence_contract_path=contract_path,
        readiness_csv_path=readiness_path,
    )

    validate_reviewable_findings_artifact(artifact)
    assert artifact["source_artifact_ids"] == [
        contract_path.as_posix(),
        readiness_path.as_posix(),
    ]
    finding_ids = {finding["finding_id"] for finding in artifact["findings"]}
    assert finding_ids.isdisjoint(_FIXTURE_FINDING_IDS)
    assert "finding:OR-PERS:FY2024:extraction-readiness" in finding_ids
    blocked = next(finding for finding in artifact["findings"] if finding["entity"] == "NY-CTRS")
    assert blocked["value"] == 0.0
    assert blocked["severity"] == "warning"
    assert artifact["total_candidate_findings"] == 2
    assert artifact["truncated"] is False


@pytest.mark.parametrize(
    ("contract_arg", "readiness_arg"),
    [
        ("/nonexistent/path.json", "/nonexistent/readiness.csv"),
        ("/nonexistent/path.json", None),
        (None, "/nonexistent/readiness.csv"),
    ],
)
def test_build_from_sources_raises_when_source_artifacts_missing(
    contract_arg: str | None, readiness_arg: str | None
) -> None:
    with pytest.raises(ReviewableFindingsArtifactError):
        build_extraction_quality_dashboard_artifact(
            persistence_contract_path=contract_arg,
            readiness_csv_path=readiness_arg,
        )


def test_build_from_sources_raises_on_malformed_persistence_contract(tmp_path: Path) -> None:
    contract_path = tmp_path / "persistence_contract.json"
    contract_path.write_text("not-json", encoding="utf-8")
    readiness_path = tmp_path / "readiness.csv"
    readiness_path.write_text("plan_id,plan_period\nOR-PERS,FY2024\n", encoding="utf-8")

    with pytest.raises(ReviewableFindingsArtifactError, match="failed to parse"):
        build_extraction_quality_dashboard_artifact(
            persistence_contract_path=contract_path,
            readiness_csv_path=readiness_path,
        )


def test_build_from_sources_raises_when_contract_omits_required_columns(
    tmp_path: Path,
) -> None:
    contract_path = tmp_path / "persistence_contract.json"
    contract_path.write_text(
        json.dumps({"staging_core_metrics": ["plan_id", "metric_name"]}),
        encoding="utf-8",
    )
    readiness_path = tmp_path / "readiness.csv"
    readiness_path.write_text(
        "plan_id,plan_period,is_extraction_ready\nOR-PERS,FY2024,true\n",
        encoding="utf-8",
    )

    with pytest.raises(ReviewableFindingsArtifactError, match="plan_period"):
        build_extraction_quality_dashboard_artifact(
            persistence_contract_path=contract_path,
            readiness_csv_path=readiness_path,
        )


def test_build_from_sources_skips_compare_action_for_single_finding(tmp_path: Path) -> None:
    contract_path, readiness_path = _write_real_sources(tmp_path)
    readiness_path.write_text(
        "plan_id,plan_period,cohort,system_type,official_resolution_state,"
        "source_authority_tier,mismatch_reason,extraction_blocker_reason,"
        "is_extraction_ready,readiness_state\n"
        "OR-PERS,FY2024,west,public,resolved,tier_1,,,true,ready\n",
        encoding="utf-8",
    )

    artifact = build_extraction_quality_dashboard_artifact(
        persistence_contract_path=contract_path,
        readiness_csv_path=readiness_path,
    )

    validate_reviewable_findings_artifact(artifact)
    assert [action["action"] for action in artifact["langchain_actions"]] == ["explain"]


def test_build_from_sources_marks_and_warns_when_findings_are_truncated(
    tmp_path: Path,
) -> None:
    contract_path, readiness_path = _write_real_sources(tmp_path)
    rows = [
        "plan_id,plan_period,cohort,system_type,official_resolution_state,"
        "source_authority_tier,mismatch_reason,extraction_blocker_reason,"
        "is_extraction_ready,readiness_state"
    ]
    rows.extend(
        f"PLAN-{index},FY2024,west,public,resolved,tier_1,,,true,ready" for index in range(30)
    )
    readiness_path.write_text("\n".join(rows) + "\n", encoding="utf-8")

    with pytest.warns(RuntimeWarning, match="truncated"):
        artifact = build_extraction_quality_dashboard_artifact(
            persistence_contract_path=contract_path,
            readiness_csv_path=readiness_path,
        )

    validate_reviewable_findings_artifact(artifact)
    assert artifact["total_candidate_findings"] == 30
    assert artifact["truncated"] is True
    assert len(artifact["findings"]) == 25


def test_build_from_sources_raises_when_readiness_csv_has_no_usable_rows(tmp_path: Path) -> None:
    contract_path, _ = _write_real_sources(tmp_path)
    readiness_path = tmp_path / "empty_readiness.csv"
    readiness_path.write_text(
        "plan_id,plan_period,is_extraction_ready\n,,\n",
        encoding="utf-8",
    )

    with pytest.raises(ReviewableFindingsArtifactError, match="no rows"):
        build_extraction_quality_dashboard_artifact(
            persistence_contract_path=contract_path,
            readiness_csv_path=readiness_path,
        )
