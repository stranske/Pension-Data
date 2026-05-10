"""Contract helpers for the first static UI/LangChain review artifact."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal

REVIEWABLE_FINDINGS_SCHEMA_VERSION = 1
REVIEWABLE_FINDINGS_ARTIFACT_TYPE = "pension_data.reviewable_findings"
REVIEWABLE_FINDINGS_ARTIFACT_PATH = (
    "docs/data/reviewable-findings/extraction-quality-dashboard.json"
)
REVIEWABLE_FINDINGS_SCHEMA_PATH = "docs/data/reviewable-findings/findings.schema.json"

ReviewFindingSeverity = Literal["info", "warning", "blocker"]

REQUIRED_ARTIFACT_FIELDS: tuple[str, ...] = (
    "artifact_type",
    "schema_version",
    "artifact_id",
    "generated_at",
    "source_artifact_ids",
    "slice",
    "findings",
    "langchain_actions",
)
REQUIRED_SLICE_FIELDS: tuple[str, ...] = (
    "slice_id",
    "title",
    "metric_family",
    "description",
)
REQUIRED_FINDING_FIELDS: tuple[str, ...] = (
    "finding_id",
    "entity",
    "period",
    "metric_family",
    "metric",
    "value",
    "confidence",
    "provenance_refs",
    "citations",
)
ALLOWED_SEVERITIES: frozenset[str] = frozenset({"info", "warning", "blocker"})
ALLOWED_LANGCHAIN_ACTIONS: frozenset[str] = frozenset({"explain", "compare"})


class ReviewableFindingsArtifactError(ValueError):
    """Raised when a reviewable findings artifact violates the contract."""


def reviewable_findings_schema() -> dict[str, object]:
    """Return the machine-readable contract for static UI and LangChain consumers."""
    return {
        "artifact_type": REVIEWABLE_FINDINGS_ARTIFACT_TYPE,
        "schema_version": REVIEWABLE_FINDINGS_SCHEMA_VERSION,
        "artifact_path": REVIEWABLE_FINDINGS_ARTIFACT_PATH,
        "required_artifact_fields": list(REQUIRED_ARTIFACT_FIELDS),
        "slice": {
            "required_fields": list(REQUIRED_SLICE_FIELDS),
            "first_slice": "extraction_quality_dashboard",
        },
        "findings": {
            "required_fields": list(REQUIRED_FINDING_FIELDS),
            "allowed_severity": sorted(ALLOWED_SEVERITIES),
            "required_filter_fields": ["entity", "period", "metric_family", "confidence"],
        },
        "langchain_actions": {
            "allowed_actions": sorted(ALLOWED_LANGCHAIN_ACTIONS),
            "required_request_fields": ["action", "question", "finding_ids"],
            "required_output_fields": [
                "request_id",
                "generated_at",
                "summary",
                "citations",
                "artifact_path",
            ],
        },
    }


def _require_mapping(value: object, *, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReviewableFindingsArtifactError(f"{path} must be an object")
    return value


def _require_string(value: object, *, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ReviewableFindingsArtifactError(f"{path} must be a non-empty string")
    return value


def _require_string_sequence(value: object, *, path: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ReviewableFindingsArtifactError(f"{path} must be a list of strings")
    normalized: list[str] = []
    for index, item in enumerate(value):
        normalized.append(_require_string(item, path=f"{path}[{index}]"))
    if not normalized:
        raise ReviewableFindingsArtifactError(f"{path} must not be empty")
    return tuple(normalized)


def _validate_required_fields(
    payload: Mapping[str, Any], *, required_fields: Sequence[str], path: str
) -> None:
    missing = [field for field in required_fields if field not in payload]
    if missing:
        joined = ", ".join(missing)
        raise ReviewableFindingsArtifactError(f"{path} missing required fields: {joined}")


def validate_reviewable_findings_artifact(artifact: Mapping[str, Any]) -> None:
    """Validate a reviewable findings artifact before publishing it to the static path."""
    _validate_required_fields(
        artifact,
        required_fields=REQUIRED_ARTIFACT_FIELDS,
        path="artifact",
    )
    if artifact["artifact_type"] != REVIEWABLE_FINDINGS_ARTIFACT_TYPE:
        raise ReviewableFindingsArtifactError(
            f"artifact_type must be {REVIEWABLE_FINDINGS_ARTIFACT_TYPE}"
        )
    if artifact["schema_version"] != REVIEWABLE_FINDINGS_SCHEMA_VERSION:
        raise ReviewableFindingsArtifactError(
            f"schema_version must be {REVIEWABLE_FINDINGS_SCHEMA_VERSION}"
        )
    _require_string(artifact["artifact_id"], path="artifact.artifact_id")
    _require_string(artifact["generated_at"], path="artifact.generated_at")
    _require_string_sequence(
        artifact["source_artifact_ids"],
        path="artifact.source_artifact_ids",
    )

    slice_payload = _require_mapping(artifact["slice"], path="artifact.slice")
    _validate_required_fields(
        slice_payload,
        required_fields=REQUIRED_SLICE_FIELDS,
        path="artifact.slice",
    )
    for field in REQUIRED_SLICE_FIELDS:
        _require_string(slice_payload[field], path=f"artifact.slice.{field}")

    findings = artifact["findings"]
    if not isinstance(findings, Sequence) or isinstance(findings, (str, bytes, bytearray)):
        raise ReviewableFindingsArtifactError("artifact.findings must be a list")
    if not findings:
        raise ReviewableFindingsArtifactError("artifact.findings must not be empty")
    for index, item in enumerate(findings):
        finding = _require_mapping(item, path=f"artifact.findings[{index}]")
        _validate_required_fields(
            finding,
            required_fields=REQUIRED_FINDING_FIELDS,
            path=f"artifact.findings[{index}]",
        )
        for field in (
            "finding_id",
            "entity",
            "period",
            "metric_family",
            "metric",
        ):
            _require_string(finding[field], path=f"artifact.findings[{index}].{field}")
        confidence = finding["confidence"]
        if not isinstance(confidence, int | float) or not 0 <= confidence <= 1:
            raise ReviewableFindingsArtifactError(
                f"artifact.findings[{index}].confidence must be between 0 and 1"
            )
        severity = finding.get("severity", "info")
        if severity not in ALLOWED_SEVERITIES:
            raise ReviewableFindingsArtifactError(
                f"artifact.findings[{index}].severity must be one of "
                f"{sorted(ALLOWED_SEVERITIES)}"
            )
        _require_string_sequence(
            finding["provenance_refs"],
            path=f"artifact.findings[{index}].provenance_refs",
        )
        _require_string_sequence(
            finding["citations"],
            path=f"artifact.findings[{index}].citations",
        )

    actions = artifact["langchain_actions"]
    if not isinstance(actions, Sequence) or isinstance(actions, (str, bytes, bytearray)):
        raise ReviewableFindingsArtifactError("artifact.langchain_actions must be a list")
    available_actions = set()
    for index, item in enumerate(actions):
        action = _require_mapping(item, path=f"artifact.langchain_actions[{index}]")
        name = _require_string(
            action.get("action"),
            path=f"artifact.langchain_actions[{index}].action",
        )
        if name not in ALLOWED_LANGCHAIN_ACTIONS:
            raise ReviewableFindingsArtifactError(
                f"artifact.langchain_actions[{index}].action must be one of "
                f"{sorted(ALLOWED_LANGCHAIN_ACTIONS)}"
            )
        available_actions.add(name)
    if available_actions != ALLOWED_LANGCHAIN_ACTIONS:
        raise ReviewableFindingsArtifactError(
            "artifact.langchain_actions must include explain and compare"
        )
