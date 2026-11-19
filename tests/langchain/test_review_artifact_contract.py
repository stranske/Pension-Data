"""Contract tests for the first static UI/LangChain review artifact."""

from __future__ import annotations

import json
import tempfile
import tomllib
from dataclasses import fields
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import call, patch

import pytest
from scripts.langchain import build_reviewable_findings_artifact as artifact_cli

from pension_data.langchain.findings_compare import CompareMetadata
from pension_data.langchain.findings_explain import ExplainMetadata
from pension_data.langchain.findings_export import FindingsExportArtifact
from pension_data.langchain.review_artifact import (
    REVIEWABLE_FINDINGS_ARTIFACT_PATH,
    REVIEWABLE_FINDINGS_SCHEMA_PATH,
    ReviewableFindingsArtifactError,
    _finding_from_readiness_row,
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


def test_pyproject_exposes_reviewable_findings_generator_command() -> None:
    pyproject_path = REPO_ROOT / "pyproject.toml"
    pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))

    scripts = pyproject["project"]["scripts"]
    assert scripts["reviewable-findings-artifact"] == (
        "scripts.langchain.build_reviewable_findings_artifact:main"
    )


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


def test_docs_data_readme_pins_generator_command_and_path() -> None:
    readme_path = REPO_ROOT / "docs/data/reviewable-findings/README.md"
    text = readme_path.read_text(encoding="utf-8")
    assert "uv run reviewable-findings-artifact" in text
    assert REVIEWABLE_FINDINGS_ARTIFACT_PATH in text


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


def test_build_from_sources_fills_utc_timestamps_when_omitted(tmp_path: Path) -> None:
    contract_path, readiness_path = _write_real_sources(tmp_path)

    with patch("pension_data.langchain.review_artifact.datetime") as mock_datetime:
        mock_datetime.now.return_value = datetime(2026, 11, 18, 22, 30, 0, tzinfo=UTC)
        artifact = build_extraction_quality_dashboard_artifact(
            persistence_contract_path=contract_path,
            readiness_csv_path=readiness_path,
        )

    validate_reviewable_findings_artifact(artifact)
    assert mock_datetime.now.call_args_list == [call(UTC), call(UTC)]
    assert artifact["generated_at"] == "2026-11-18T22:30:00Z"
    assert artifact["artifact_id"] == "extraction-quality-dashboard:2026-11-18"


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


def test_build_from_sources_raises_when_readiness_csv_missing(tmp_path: Path) -> None:
    contract_path, _ = _write_real_sources(tmp_path)
    missing_path = tmp_path / "missing_readiness.csv"
    with pytest.raises(ReviewableFindingsArtifactError, match="source artifact not found"):
        build_extraction_quality_dashboard_artifact(
            persistence_contract_path=contract_path,
            readiness_csv_path=missing_path,
        )


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


_MULTI_SOURCE_HEADER = (
    "plan_id,plan_period,cohort,system_type,official_resolution_state,"
    "source_authority_tier,mismatch_reason,extraction_blocker_reason,"
    "is_extraction_ready,readiness_state"
)
_MULTI_SOURCE_ROWS = (
    "CA-PERS,FY2024,state,public,available_official,official,,,true,ready",
    "CA-PERS,FY2024,state,public,available_non_official_only,"
    "high-confidence-third-party,non_official_only,non_official_only,false,blocked_source",
    "CA-PERS,FY2023,state,public,available_official,official,stale_period,"
    "stale_period,false,blocked_quality",
    "TX-ERS,FY2024,state,public,available_non_official_only,"
    "high-confidence-third-party,non_official_only,non_official_only,false,blocked_source",
)


def _build_from_rows(tmp_path: Path, rows: tuple[str, ...]) -> dict[str, Any]:
    contract_path, readiness_path = _write_real_sources(tmp_path)
    readiness_path.write_text("\n".join((_MULTI_SOURCE_HEADER, *rows)) + "\n", encoding="utf-8")
    return build_extraction_quality_dashboard_artifact(
        generated_at="2026-11-17T00:00:00Z",
        artifact_date="2026-11-17",
        persistence_contract_path=contract_path,
        readiness_csv_path=readiness_path,
    )


def test_multi_source_plan_period_findings_are_unambiguous(tmp_path: Path) -> None:
    forward = _build_from_rows(tmp_path, _MULTI_SOURCE_ROWS)
    reversed_rows = _build_from_rows(tmp_path, tuple(reversed(_MULTI_SOURCE_ROWS)))

    validate_reviewable_findings_artifact(forward)
    assert forward == reversed_rows

    finding_ids = [finding["finding_id"] for finding in forward["findings"]]
    assert finding_ids == [
        "finding:CA-PERS:FY2023:extraction-readiness",
        "finding:CA-PERS:FY2024:extraction-readiness",
        "finding:TX-ERS:FY2024:extraction-readiness",
    ]
    assert forward["total_candidate_findings"] == 3

    findings_by_id = {finding["finding_id"]: finding for finding in forward["findings"]}
    readiness_ref = forward["source_artifact_ids"][1]

    ca_2023 = findings_by_id["finding:CA-PERS:FY2023:extraction-readiness"]
    assert ca_2023["value"] == 0.0
    assert ca_2023["severity"] == "warning"
    assert ca_2023["provenance_refs"] == [
        f"{readiness_ref}#CA-PERS:FY2023",
        f"{readiness_ref}#CA-PERS:FY2023:source_authority_tier=official"
        ":readiness_state=blocked_quality:extraction_blocker_reason=stale_period",
    ]

    ca_2024 = findings_by_id["finding:CA-PERS:FY2024:extraction-readiness"]
    assert ca_2024["value"] == 1.0
    assert ca_2024["severity"] == "info"
    assert ca_2024["provenance_refs"] == [
        f"{readiness_ref}#CA-PERS:FY2024",
        f"{readiness_ref}#CA-PERS:FY2024:source_authority_tier=high-confidence-third-party"
        ":readiness_state=blocked_source:extraction_blocker_reason=non_official_only",
        f"{readiness_ref}#CA-PERS:FY2024:source_authority_tier=official"
        ":readiness_state=ready:extraction_blocker_reason=none",
    ]
    assert ca_2024["citations"] == forward["source_artifact_ids"]

    compare = next(a for a in forward["langchain_actions"] if a["action"] == "compare")
    assert len(set(compare["finding_ids"])) == 2

    duplicate_ids = json.loads(json.dumps(forward))
    duplicate_ids["findings"].append(dict(duplicate_ids["findings"][0]))
    with pytest.raises(ReviewableFindingsArtifactError, match="duplicates"):
        validate_reviewable_findings_artifact(duplicate_ids)

    duplicate_compare = json.loads(json.dumps(forward))
    for action in duplicate_compare["langchain_actions"]:
        if action["action"] == "compare":
            action["finding_ids"] = [finding_ids[0], finding_ids[0]]
    with pytest.raises(ReviewableFindingsArtifactError, match="two distinct findings"):
        validate_reviewable_findings_artifact(duplicate_compare)


def test_multi_source_plan_period_without_official_ready_source_is_warning(
    tmp_path: Path,
) -> None:
    artifact = _build_from_rows(
        tmp_path,
        (
            "CA-PERS,FY2024,state,public,not_found,high-confidence-third-party,,"
            "official_source_unresolved,false,blocked_source",
            "CA-PERS,FY2024,state,public,available_official,official,stale_period,"
            "stale_period,false,blocked_quality",
        ),
    )

    validate_reviewable_findings_artifact(artifact)
    assert len(artifact["findings"]) == 1
    finding = artifact["findings"][0]
    assert finding["finding_id"] == "finding:CA-PERS:FY2024:extraction-readiness"
    assert finding["value"] == 0.0
    assert finding["severity"] == "warning"
    assert any("official_source_unresolved" in ref for ref in finding["provenance_refs"])
    assert any("stale_period" in ref for ref in finding["provenance_refs"])
    assert [action["action"] for action in artifact["langchain_actions"]] == ["explain"]


def test_validator_rejects_duplicate_finding_ids() -> None:
    artifact = _valid_artifact()
    artifact["findings"][1]["finding_id"] = artifact["findings"][0]["finding_id"]
    for action in artifact["langchain_actions"]:
        action["finding_ids"] = [artifact["findings"][0]["finding_id"]]

    with pytest.raises(ReviewableFindingsArtifactError, match="duplicates"):
        validate_reviewable_findings_artifact(artifact)


def test_validator_rejects_compare_without_two_distinct_findings() -> None:
    artifact = _valid_artifact()
    compare = next(a for a in artifact["langchain_actions"] if a["action"] == "compare")
    compare["finding_ids"] = [compare["finding_ids"][0]] * 2

    with pytest.raises(ReviewableFindingsArtifactError, match="two distinct findings"):
        validate_reviewable_findings_artifact(artifact)


def test_validator_rejects_wrong_artifact_type_and_schema_version() -> None:
    artifact = _valid_artifact()
    artifact["artifact_type"] = "other.artifact"
    with pytest.raises(ReviewableFindingsArtifactError, match="artifact_type"):
        validate_reviewable_findings_artifact(artifact)

    artifact = _valid_artifact()
    artifact["schema_version"] = 99
    with pytest.raises(ReviewableFindingsArtifactError, match="schema_version"):
        validate_reviewable_findings_artifact(artifact)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("slice", "not-an-object", "must be an object"),
        ("findings", (), "must not be empty"),
        ("findings", "not-a-list", "must be a list"),
        ("langchain_actions", "not-a-list", "must be a list"),
    ],
)
def test_validator_rejects_malformed_artifact_sections(
    field: str, value: object, message: str
) -> None:
    artifact = _valid_artifact()
    artifact[field] = value
    with pytest.raises(ReviewableFindingsArtifactError, match=message):
        validate_reviewable_findings_artifact(artifact)


def test_validator_rejects_invalid_severity_and_unknown_langchain_action() -> None:
    artifact = _valid_artifact()
    artifact["findings"][0]["severity"] = "critical"
    with pytest.raises(ReviewableFindingsArtifactError, match="severity"):
        validate_reviewable_findings_artifact(artifact)

    artifact = _valid_artifact()
    artifact["langchain_actions"][0]["action"] = "summarize"
    with pytest.raises(ReviewableFindingsArtifactError, match="action must be one of"):
        validate_reviewable_findings_artifact(artifact)


def test_validator_requires_compare_when_multiple_findings_exist() -> None:
    artifact = _valid_artifact()
    artifact["langchain_actions"] = [artifact["langchain_actions"][0]]
    with pytest.raises(ReviewableFindingsArtifactError, match="compare and explain"):
        validate_reviewable_findings_artifact(artifact)


def test_build_from_sources_rejects_missing_contract_file(tmp_path: Path) -> None:
    missing = tmp_path / "missing_contract.json"
    readiness = tmp_path / "readiness.csv"
    readiness.write_text(
        "plan_id,plan_period,is_extraction_ready\nA,FY2024,true\n", encoding="utf-8"
    )
    with pytest.raises(ReviewableFindingsArtifactError, match="not found"):
        build_extraction_quality_dashboard_artifact(
            persistence_contract_path=missing,
            readiness_csv_path=readiness,
        )


def test_build_from_sources_rejects_invalid_contract_json(tmp_path: Path) -> None:
    contract_path = tmp_path / "contract.json"
    contract_path.write_text("{not json", encoding="utf-8")
    readiness = tmp_path / "readiness.csv"
    readiness.write_text(
        "plan_id,plan_period,is_extraction_ready\nA,FY2024,true\n", encoding="utf-8"
    )
    with pytest.raises(ReviewableFindingsArtifactError, match="failed to parse as JSON"):
        build_extraction_quality_dashboard_artifact(
            persistence_contract_path=contract_path,
            readiness_csv_path=readiness,
        )


def test_build_from_sources_rejects_non_object_contract_payload(tmp_path: Path) -> None:
    contract_path = tmp_path / "contract.json"
    contract_path.write_text(json.dumps(["not", "an", "object"]), encoding="utf-8")
    readiness = tmp_path / "readiness.csv"
    readiness.write_text(
        "plan_id,plan_period,is_extraction_ready\nA,FY2024,true\n", encoding="utf-8"
    )
    with pytest.raises(ReviewableFindingsArtifactError, match="must be a JSON object"):
        build_extraction_quality_dashboard_artifact(
            persistence_contract_path=contract_path,
            readiness_csv_path=readiness,
        )


def test_build_from_sources_rejects_contract_without_readiness_columns(tmp_path: Path) -> None:
    contract_path = tmp_path / "contract.json"
    contract_path.write_text(json.dumps({"metadata": "only"}), encoding="utf-8")
    readiness = tmp_path / "readiness.csv"
    readiness.write_text(
        "plan_id,plan_period,is_extraction_ready\nA,FY2024,true\n", encoding="utf-8"
    )
    with pytest.raises(
        ReviewableFindingsArtifactError, match="must define source_authority_readiness"
    ):
        build_extraction_quality_dashboard_artifact(
            persistence_contract_path=contract_path,
            readiness_csv_path=readiness,
        )


def test_build_from_sources_rejects_contract_missing_plan_columns(tmp_path: Path) -> None:
    contract_path = tmp_path / "contract.json"
    contract_path.write_text(
        json.dumps({"staging_core_metrics": ["metric_name", "is_extraction_ready"]}),
        encoding="utf-8",
    )
    readiness = tmp_path / "readiness.csv"
    readiness.write_text(
        "plan_id,plan_period,is_extraction_ready\nA,FY2024,true\n", encoding="utf-8"
    )
    with pytest.raises(ReviewableFindingsArtifactError, match="missing required columns"):
        build_extraction_quality_dashboard_artifact(
            persistence_contract_path=contract_path,
            readiness_csv_path=readiness,
        )


@pytest.mark.parametrize(
    ("csv_body", "message"),
    [
        ("", "no header row"),
        ("metric,value\na,b\n", "missing required columns"),
        ("plan_id,plan_period,is_extraction_ready\n", "no rows"),
    ],
)
def test_build_from_sources_rejects_unusable_readiness_csv(
    tmp_path: Path, csv_body: str, message: str
) -> None:
    contract_path, _ = _write_real_sources(tmp_path)
    readiness = tmp_path / "readiness.csv"
    readiness.write_text(csv_body, encoding="utf-8")
    with pytest.raises(ReviewableFindingsArtifactError, match=message):
        build_extraction_quality_dashboard_artifact(
            persistence_contract_path=contract_path,
            readiness_csv_path=readiness,
        )


def test_build_from_sources_uses_readiness_key_alias_in_contract(tmp_path: Path) -> None:
    contract_path = tmp_path / "contract.json"
    contract_path.write_text(
        json.dumps({"readiness": ["plan_id", "plan_period", "is_extraction_ready"]}),
        encoding="utf-8",
    )
    readiness = tmp_path / "readiness.csv"
    readiness.write_text(
        "plan_id,plan_period,is_extraction_ready\nOR-PERS,FY2024,true\n",
        encoding="utf-8",
    )
    artifact = build_extraction_quality_dashboard_artifact(
        persistence_contract_path=contract_path,
        readiness_csv_path=readiness,
    )
    validate_reviewable_findings_artifact(artifact)
    assert artifact["findings"][0]["entity"] == "OR-PERS"


def test_finding_from_readiness_row_handles_missing_and_present_identity(tmp_path: Path) -> None:
    contract_path, readiness_path = _write_real_sources(tmp_path)
    assert (
        _finding_from_readiness_row(
            {"plan_id": " ", "plan_period": ""},
            contract_path=contract_path,
            readiness_path=readiness_path,
        )
        is None
    )
    finding = _finding_from_readiness_row(
        {"plan_id": "OR-PERS", "plan_period": "FY2024", "is_extraction_ready": "true"},
        contract_path=contract_path,
        readiness_path=readiness_path,
    )
    assert finding is not None
    assert finding["finding_id"] == "finding:OR-PERS:FY2024:extraction-readiness"


def test_build_from_sources_accepts_source_authority_readiness_contract_key(tmp_path: Path) -> None:
    contract_path = tmp_path / "contract.json"
    contract_path.write_text(
        json.dumps(
            {"source_authority_readiness": ["plan_id", "plan_period", "is_extraction_ready"]}
        ),
        encoding="utf-8",
    )
    readiness = tmp_path / "readiness.csv"
    readiness.write_text(
        "plan_id,plan_period,is_extraction_ready\nOR-PERS,FY2024,true\n",
        encoding="utf-8",
    )
    artifact = build_extraction_quality_dashboard_artifact(
        persistence_contract_path=contract_path,
        readiness_csv_path=readiness,
    )
    validate_reviewable_findings_artifact(artifact)


def test_build_from_sources_rejects_non_list_contract_columns(tmp_path: Path) -> None:
    contract_path = tmp_path / "contract.json"
    contract_path.write_text(json.dumps({"staging_core_metrics": "plan_id"}), encoding="utf-8")
    readiness = tmp_path / "readiness.csv"
    readiness.write_text(
        "plan_id,plan_period,is_extraction_ready\nA,FY2024,true\n", encoding="utf-8"
    )
    with pytest.raises(ReviewableFindingsArtifactError, match="must be a list of column names"):
        build_extraction_quality_dashboard_artifact(
            persistence_contract_path=contract_path,
            readiness_csv_path=readiness,
        )


def test_build_from_sources_rejects_malformed_readiness_csv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import csv

    contract_path, readiness_path = _write_real_sources(tmp_path)

    def _broken_dict_reader(*_args: object, **_kwargs: object) -> object:
        raise csv.Error("broken csv row")

    monkeypatch.setattr(csv, "DictReader", _broken_dict_reader)
    with pytest.raises(ReviewableFindingsArtifactError, match="failed to parse as CSV"):
        build_extraction_quality_dashboard_artifact(
            persistence_contract_path=contract_path,
            readiness_csv_path=readiness_path,
        )


def test_build_marks_not_found_resolution_as_not_official_ready(tmp_path: Path) -> None:
    contract_path, readiness_path = _write_real_sources(tmp_path)
    readiness_path.write_text(
        "plan_id,plan_period,cohort,system_type,official_resolution_state,"
        "source_authority_tier,mismatch_reason,extraction_blocker_reason,"
        "is_extraction_ready,readiness_state\n"
        "CA-PERS,FY2024,state,public,not_found,official,,,true,ready\n"
        "CA-PERS,FY2024,state,public,not_found,official,,,false,blocked\n",
        encoding="utf-8",
    )
    artifact = build_extraction_quality_dashboard_artifact(
        persistence_contract_path=contract_path,
        readiness_csv_path=readiness_path,
    )
    finding = artifact["findings"][0]
    assert finding["value"] == 0.0
    assert finding["severity"] == "warning"


def test_build_from_sources_rejects_unreadable_contract_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    contract_path = tmp_path / "contract.json"
    contract_path.write_text("{}", encoding="utf-8")
    readiness = tmp_path / "readiness.csv"
    readiness.write_text(
        "plan_id,plan_period,is_extraction_ready\nA,FY2024,true\n", encoding="utf-8"
    )

    def fail_read_text(*_args: object, **_kwargs: object) -> str:
        raise OSError(13, "permission denied")

    monkeypatch.setattr(Path, "read_text", fail_read_text)
    with pytest.raises(ReviewableFindingsArtifactError, match="unreadable"):
        build_extraction_quality_dashboard_artifact(
            persistence_contract_path=contract_path,
            readiness_csv_path=readiness,
        )


def test_build_from_sources_rejects_unreadable_readiness_csv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    contract_path, readiness_path = _write_real_sources(tmp_path)

    real_open = Path.open

    def fail_open(self: Path, *args: object, **kwargs: object):
        if self == readiness_path:
            raise OSError(13, "permission denied")
        return real_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", fail_open)
    with pytest.raises(ReviewableFindingsArtifactError, match="unreadable"):
        build_extraction_quality_dashboard_artifact(
            persistence_contract_path=contract_path,
            readiness_csv_path=readiness_path,
        )


def test_build_from_sources_skips_rows_without_plan_identity(tmp_path: Path) -> None:
    contract_path, readiness_path = _write_real_sources(tmp_path)
    readiness_path.write_text(
        "plan_id,plan_period,is_extraction_ready\n" ",,true\n" "OR-PERS,FY2024,true\n",
        encoding="utf-8",
    )
    artifact = build_extraction_quality_dashboard_artifact(
        persistence_contract_path=contract_path,
        readiness_csv_path=readiness_path,
    )
    assert len(artifact["findings"]) == 1
    assert artifact["findings"][0]["entity"] == "OR-PERS"
