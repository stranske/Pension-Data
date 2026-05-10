"""Contract tests for the first static UI/LangChain review artifact."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from pension_data.langchain.review_artifact import (
    REVIEWABLE_FINDINGS_ARTIFACT_PATH,
    REVIEWABLE_FINDINGS_ARTIFACT_TYPE,
    REVIEWABLE_FINDINGS_SCHEMA_PATH,
    REVIEWABLE_FINDINGS_SCHEMA_VERSION,
    ReviewableFindingsArtifactError,
    reviewable_findings_schema,
    validate_reviewable_findings_artifact,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def _valid_artifact() -> dict[str, Any]:
    return {
        "artifact_type": REVIEWABLE_FINDINGS_ARTIFACT_TYPE,
        "schema_version": REVIEWABLE_FINDINGS_SCHEMA_VERSION,
        "artifact_id": "extraction-quality-dashboard:2026-05-10",
        "generated_at": "2026-05-10T03:20:00Z",
        "source_artifact_ids": [
            "extraction_persistence/persistence_contract.json",
            "coverage/source_authority_readiness.csv",
        ],
        "slice": {
            "slice_id": "extraction_quality_dashboard",
            "title": "Extraction Quality Dashboard",
            "metric_family": "extraction_quality",
            "description": "Review extraction confidence, blockers, and source-backed citations.",
        },
        "findings": [
            {
                "finding_id": "finding:ca-pers:fy2024:funded-ratio",
                "entity": "CA-PERS",
                "period": "FY2024",
                "metric_family": "funded_status",
                "metric": "funded_ratio",
                "value": 0.81,
                "confidence": 0.96,
                "severity": "info",
                "provenance_refs": ["doc:ca-pers-2024#page=52"],
                "citations": ["CA-PERS ACFR FY2024 p.52"],
            },
            {
                "finding_id": "finding:ca-pers:fy2023:funded-ratio",
                "entity": "CA-PERS",
                "period": "FY2023",
                "metric_family": "funded_status",
                "metric": "funded_ratio",
                "value": 0.79,
                "confidence": 0.93,
                "severity": "info",
                "provenance_refs": ["doc:ca-pers-2023#page=49"],
                "citations": ["CA-PERS ACFR FY2023 p.49"],
            },
        ],
        "langchain_actions": [
            {
                "action": "explain",
                "question": "Explain the confidence drivers for this finding",
                "finding_ids": ["finding:ca-pers:fy2024:funded-ratio"],
            },
            {
                "action": "compare",
                "question": "Compare confidence against the prior period",
                "finding_ids": [
                    "finding:ca-pers:fy2024:funded-ratio",
                    "finding:ca-pers:fy2023:funded-ratio",
                ],
            },
        ],
    }


def test_schema_file_matches_python_contract() -> None:
    schema_path = REPO_ROOT / REVIEWABLE_FINDINGS_SCHEMA_PATH
    schema = json.loads(schema_path.read_text())

    assert schema == reviewable_findings_schema()
    assert schema["artifact_path"] == REVIEWABLE_FINDINGS_ARTIFACT_PATH
    assert schema["slice"]["first_slice"] == "extraction_quality_dashboard"
    assert "confidence" in schema["findings"]["required_filter_fields"]


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
