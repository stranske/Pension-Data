"""Contract helpers for the first static UI/LangChain review artifact."""

from __future__ import annotations

import csv
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

REVIEWABLE_FINDINGS_SCHEMA_VERSION = 1
REVIEWABLE_FINDINGS_ARTIFACT_TYPE = "pension_data.reviewable_findings"
REVIEWABLE_FINDINGS_ARTIFACT_PATH = (
    "docs/data/reviewable-findings/extraction-quality-dashboard.json"
)
REVIEWABLE_FINDINGS_SCHEMA_PATH = "docs/data/reviewable-findings/findings.schema.json"
REVIEWABLE_FINDINGS_FIRST_SLICE_ID = "extraction_quality_dashboard"

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
            "first_slice": REVIEWABLE_FINDINGS_FIRST_SLICE_ID,
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
    if slice_payload["slice_id"] != REVIEWABLE_FINDINGS_FIRST_SLICE_ID:
        raise ReviewableFindingsArtifactError(
            "artifact.slice.slice_id must be " f"{REVIEWABLE_FINDINGS_FIRST_SLICE_ID}"
        )

    findings = artifact["findings"]
    if not isinstance(findings, Sequence) or isinstance(findings, (str, bytes, bytearray)):
        raise ReviewableFindingsArtifactError("artifact.findings must be a list")
    if not findings:
        raise ReviewableFindingsArtifactError("artifact.findings must not be empty")
    finding_ids: set[str] = set()
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
        finding_ids.add(finding["finding_id"])
        confidence = finding["confidence"]
        if (
            isinstance(confidence, bool)
            or not isinstance(confidence, int | float)
            or not 0 <= confidence <= 1
        ):
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
        _require_string(
            action.get("question"),
            path=f"artifact.langchain_actions[{index}].question",
        )
        action_finding_ids = _require_string_sequence(
            action.get("finding_ids"),
            path=f"artifact.langchain_actions[{index}].finding_ids",
        )
        unknown_finding_ids = sorted(set(action_finding_ids) - finding_ids)
        if unknown_finding_ids:
            raise ReviewableFindingsArtifactError(
                f"artifact.langchain_actions[{index}].finding_ids reference "
                f"unknown findings: {', '.join(unknown_finding_ids)}"
            )
        available_actions.add(name)
    if available_actions != ALLOWED_LANGCHAIN_ACTIONS:
        raise ReviewableFindingsArtifactError(
            "artifact.langchain_actions must include explain and compare"
        )


MAX_REAL_DATA_FINDINGS = 25


def _normalize_generated_at(generated_at: str | None) -> str:
    if generated_at:
        return generated_at
    now = datetime.now(UTC)
    return now.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_artifact_date(artifact_date: str | None) -> str:
    if artifact_date:
        return artifact_date
    return datetime.now(UTC).date().isoformat()


def _read_persistence_contract(path: Path) -> Mapping[str, Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ReviewableFindingsArtifactError(f"source artifact not found: {path}") from exc
    except OSError as exc:
        raise ReviewableFindingsArtifactError(f"source artifact unreadable: {path}: {exc}") from exc
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ReviewableFindingsArtifactError(
            f"source artifact failed to parse as JSON: {path}: {exc.msg}"
        ) from exc
    if not isinstance(payload, Mapping):
        raise ReviewableFindingsArtifactError(
            f"persistence contract at {path} must be a JSON object"
        )
    return payload


def _read_readiness_rows(path: Path) -> list[dict[str, str]]:
    try:
        handle = path.open("r", encoding="utf-8", newline="")
    except FileNotFoundError as exc:
        raise ReviewableFindingsArtifactError(f"source artifact not found: {path}") from exc
    except OSError as exc:
        raise ReviewableFindingsArtifactError(f"source artifact unreadable: {path}: {exc}") from exc
    with handle:
        try:
            reader = csv.DictReader(handle)
            rows = [dict(row) for row in reader]
        except csv.Error as exc:
            raise ReviewableFindingsArtifactError(
                f"source artifact failed to parse as CSV: {path}: {exc}"
            ) from exc
    if not rows:
        raise ReviewableFindingsArtifactError(
            f"readiness CSV at {path} has no rows; cannot derive findings"
        )
    return rows


def _finding_from_readiness_row(
    row: Mapping[str, str], *, contract_path: Path, readiness_path: Path
) -> dict[str, Any] | None:
    plan_id = (row.get("plan_id") or "").strip()
    plan_period = (row.get("plan_period") or "").strip()
    if not plan_id or not plan_period:
        return None
    is_ready_raw = (row.get("is_extraction_ready") or "").strip().lower()
    is_ready = is_ready_raw in {"true", "1", "yes"}
    severity = "info" if is_ready else "warning"
    return {
        "finding_id": f"finding:{plan_id}:{plan_period}:extraction-readiness",
        "entity": plan_id,
        "period": plan_period,
        "metric_family": "extraction_quality",
        "metric": "extraction_readiness",
        "value": 1.0 if is_ready else 0.0,
        "confidence": 1.0,
        "severity": severity,
        "provenance_refs": [
            f"{readiness_path.as_posix()}#{plan_id}:{plan_period}",
        ],
        "citations": [
            contract_path.as_posix(),
            readiness_path.as_posix(),
        ],
    }


def _build_from_sources(
    *,
    persistence_contract_path: Path,
    readiness_csv_path: Path,
    generated_at: str,
    artifact_date: str,
) -> dict[str, Any]:
    _read_persistence_contract(persistence_contract_path)
    rows = _read_readiness_rows(readiness_csv_path)

    findings: list[dict[str, Any]] = []
    for row in rows:
        finding = _finding_from_readiness_row(
            row,
            contract_path=persistence_contract_path,
            readiness_path=readiness_csv_path,
        )
        if finding is None:
            continue
        findings.append(finding)
        if len(findings) >= MAX_REAL_DATA_FINDINGS:
            break
    if not findings:
        raise ReviewableFindingsArtifactError(
            f"readiness CSV at {readiness_csv_path} contains no rows with plan_id and plan_period"
        )

    action_finding_ids = [finding["finding_id"] for finding in findings]
    compare_finding_ids = action_finding_ids[: min(2, len(action_finding_ids))]
    return {
        "artifact_type": REVIEWABLE_FINDINGS_ARTIFACT_TYPE,
        "schema_version": REVIEWABLE_FINDINGS_SCHEMA_VERSION,
        "artifact_id": f"extraction-quality-dashboard:{artifact_date}",
        "generated_at": generated_at,
        "source_artifact_ids": [
            persistence_contract_path.as_posix(),
            readiness_csv_path.as_posix(),
        ],
        "slice": {
            "slice_id": REVIEWABLE_FINDINGS_FIRST_SLICE_ID,
            "title": "Extraction Quality Dashboard",
            "metric_family": "extraction_quality",
            "description": "Review extraction readiness derived from persistence and readiness artifacts.",
        },
        "findings": findings,
        "langchain_actions": [
            {
                "action": "explain",
                "question": "Explain the readiness drivers for this finding",
                "finding_ids": [action_finding_ids[0]],
            },
            {
                "action": "compare",
                "question": "Compare readiness against another plan period",
                "finding_ids": compare_finding_ids,
            },
        ],
    }


def build_extraction_quality_dashboard_artifact(
    *,
    generated_at: str | None = None,
    artifact_date: str | None = None,
    persistence_contract_path: str | Path | None = None,
    readiness_csv_path: str | Path | None = None,
) -> dict[str, Any]:
    """Build the extraction-quality dashboard artifact payload.

    When ``persistence_contract_path`` and ``readiness_csv_path`` are both
    provided, the generator reads them and derives finding rows from real
    extraction/readiness data. If either path is missing, unreadable, or fails
    to parse, ``ReviewableFindingsArtifactError`` is raised before any
    hardcoded fallback can execute.

    When both paths are ``None``, the legacy synthetic fixture artifact is
    returned so contract-level test coverage and the checked-in artifact at
    ``docs/data/reviewable-findings/extraction-quality-dashboard.json`` keep
    passing ``validate_reviewable_findings_artifact``. Production callers must
    pass real source paths; the generator script does so by default.
    """
    normalized_generated_at = _normalize_generated_at(generated_at)
    normalized_artifact_date = _normalize_artifact_date(artifact_date)

    if persistence_contract_path is not None or readiness_csv_path is not None:
        if persistence_contract_path is None or readiness_csv_path is None:
            raise ReviewableFindingsArtifactError(
                "both persistence_contract_path and readiness_csv_path must be provided together"
            )
        return _build_from_sources(
            persistence_contract_path=Path(persistence_contract_path),
            readiness_csv_path=Path(readiness_csv_path),
            generated_at=normalized_generated_at,
            artifact_date=normalized_artifact_date,
        )

    return {
        "artifact_type": REVIEWABLE_FINDINGS_ARTIFACT_TYPE,
        "schema_version": REVIEWABLE_FINDINGS_SCHEMA_VERSION,
        "artifact_id": f"extraction-quality-dashboard:{normalized_artifact_date}",
        "generated_at": normalized_generated_at,
        "source_artifact_ids": [
            "extraction_persistence/persistence_contract.json",
            "coverage/source_authority_readiness.csv",
        ],
        "slice": {
            "slice_id": REVIEWABLE_FINDINGS_FIRST_SLICE_ID,
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


def write_reviewable_findings_artifact(
    artifact: Mapping[str, Any],
    *,
    output_path: str | Path = REVIEWABLE_FINDINGS_ARTIFACT_PATH,
) -> Path:
    """Validate and write the artifact JSON to a deterministic output path."""
    validate_reviewable_findings_artifact(artifact)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(artifact, indent=2) + "\n", encoding="utf-8")
    return path
