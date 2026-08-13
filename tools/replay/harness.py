"""Replay harness utilities for golden-corpus regression detection.

Snapshot versioning uses two layers:
- ``schema_version`` controls the JSON envelope shape for compatibility checks.
- ``baseline_version`` controls baseline semantics expected by diff tooling.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, TypedDict

REPLAY_BASELINE_ARTIFACT_TYPE = "pension_replay_baseline"
SUPPORTED_ARTIFACT_SCHEMA_VERSION = 1
SUPPORTED_BASELINE_VERSION = "v1"
DEFAULT_DETERMINISTIC_GENERATED_AT = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class CorpusDocument:
    """Golden-corpus document passed through replay."""

    document_id: str
    content: str


@dataclass(frozen=True, slots=True)
class FieldExtraction:
    """Extracted field payload used by replay snapshots."""

    value: object
    confidence: float | None = None
    evidence: str | None = None


@dataclass(frozen=True, slots=True)
class ReplayResult:
    """Per-document extraction output from a replay run."""

    document_id: str
    fields: dict[str, FieldExtraction]


class FieldPayload(TypedDict):
    """JSON-serializable field payload in a replay snapshot."""

    value: object
    confidence: float | None
    evidence: str | None


class SnapshotDocument(TypedDict):
    """Single document row in a replay snapshot."""

    document_id: str
    fields: dict[str, FieldPayload]


class ReplaySnapshot(TypedDict):
    """Replay snapshot for baselining and regression checks."""

    artifact_type: str
    schema_version: int
    baseline_version: str
    parser_id: str
    generated_at: str
    documents: list[SnapshotDocument]


class FieldDiff(TypedDict):
    """Field-level diff output."""

    document_id: str
    field: str
    attribute: str
    baseline: object
    current: object
    classification: Literal["expected_change", "unexpected_drift"]


class DiffReport(TypedDict):
    """Structured replay diff report."""

    total_changes: int
    expected_changes: int
    unexpected_changes: int
    changes: list[FieldDiff]


class EvaluationMetadata(TypedDict):
    """Identity and thresholds required for comparable extraction evaluations."""

    corpus_id: str
    corpus_schema_version: int
    thresholds: dict[str, float]


class ExtractionAccuracyReport(TypedDict):
    """Field and document accuracy results for one comparable replay pair."""

    corpus_id: str
    corpus_schema_version: int
    corpus_size: int
    exact_matches: int
    incorrect_fields: int
    missing_fields: int
    extra_fields: int
    unscorable_fields: int
    field_precision: float
    field_recall: float
    exact_match_accuracy: float
    document_pass_rate: float
    worst_regressions: list[dict[str, str]]


_METRIC_KEYS = frozenset(
    {"field_precision", "field_recall", "exact_match_accuracy", "document_pass_rate"}
)


Parser = Callable[[CorpusDocument], Mapping[str, FieldExtraction]]


def _normalize_fields(fields: Mapping[str, FieldExtraction]) -> dict[str, FieldExtraction]:
    return {name: fields[name] for name in sorted(fields)}


def run_replay(corpus: list[CorpusDocument], parser: Parser) -> list[ReplayResult]:
    """Run parser replay over corpus with deterministic ordering."""
    ordered_documents = sorted(corpus, key=lambda item: item.document_id)
    if len(ordered_documents) != len({item.document_id for item in ordered_documents}):
        raise ValueError("corpus contains duplicate document_id values")

    replay_results: list[ReplayResult] = []
    for document in ordered_documents:
        replay_results.append(
            ReplayResult(
                document_id=document.document_id,
                fields=_normalize_fields(parser(document)),
            )
        )
    return replay_results


def _to_field_payload(field: FieldExtraction) -> FieldPayload:
    return {
        "value": field.value,
        "confidence": field.confidence,
        "evidence": field.evidence,
    }


def build_snapshot(
    replay_results: list[ReplayResult],
    *,
    baseline_version: str = SUPPORTED_BASELINE_VERSION,
    parser_id: str = "unknown",
    generated_at: datetime | None = None,
) -> ReplaySnapshot:
    """Build JSON-friendly replay snapshot from run output."""
    if baseline_version != SUPPORTED_BASELINE_VERSION:
        raise ValueError(
            "baseline_version must be " f"'{SUPPORTED_BASELINE_VERSION}' for this replay harness"
        )
    timestamp = (generated_at or DEFAULT_DETERMINISTIC_GENERATED_AT).astimezone(UTC).isoformat()
    ordered_results = sorted(replay_results, key=lambda item: item.document_id)
    if len(ordered_results) != len({item.document_id for item in ordered_results}):
        raise ValueError("replay_results contains duplicate document_id values")
    documents: list[SnapshotDocument] = []
    for replay_result in ordered_results:
        documents.append(
            {
                "document_id": replay_result.document_id,
                "fields": {
                    field_name: _to_field_payload(replay_result.fields[field_name])
                    for field_name in sorted(replay_result.fields)
                },
            }
        )
    return {
        "artifact_type": REPLAY_BASELINE_ARTIFACT_TYPE,
        "schema_version": SUPPORTED_ARTIFACT_SCHEMA_VERSION,
        "baseline_version": baseline_version,
        "parser_id": parser_id,
        "generated_at": timestamp,
        "documents": documents,
    }


def write_snapshot(path: Path, snapshot: ReplaySnapshot, *, overwrite: bool = False) -> None:
    """Persist snapshot with explicit overwrite control for baseline updates."""
    if path.exists() and not overwrite:
        raise FileExistsError(
            f"snapshot already exists at '{path}'; pass overwrite=True to replace it"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _validate_field_payload(payload: object, *, location: str) -> FieldPayload:
    if not isinstance(payload, dict):
        raise ValueError(f"{location} must be an object")
    if "value" not in payload:
        raise ValueError(f"{location} missing required key 'value'")
    confidence = payload.get("confidence")
    if confidence is not None and not isinstance(confidence, (int, float)):
        raise ValueError(f"{location}.confidence must be numeric or null")
    evidence = payload.get("evidence")
    if evidence is not None and not isinstance(evidence, str):
        raise ValueError(f"{location}.evidence must be a string or null")
    return {
        "value": payload["value"],
        "confidence": float(confidence) if isinstance(confidence, (int, float)) else None,
        "evidence": evidence,
    }


def _validate_snapshot(payload: object) -> ReplaySnapshot:
    if not isinstance(payload, dict):
        raise ValueError("snapshot payload must be a JSON object")

    if payload.get("artifact_type") != REPLAY_BASELINE_ARTIFACT_TYPE:
        raise ValueError(
            "snapshot artifact_type must equal "
            f"'{REPLAY_BASELINE_ARTIFACT_TYPE}' for this replay harness"
        )

    schema_version = payload.get("schema_version")
    if schema_version != SUPPORTED_ARTIFACT_SCHEMA_VERSION:
        raise ValueError(
            "snapshot schema_version must equal "
            f"{SUPPORTED_ARTIFACT_SCHEMA_VERSION} for this replay harness"
        )

    if payload.get("baseline_version") != SUPPORTED_BASELINE_VERSION:
        raise ValueError(
            "snapshot baseline_version must equal "
            f"'{SUPPORTED_BASELINE_VERSION}' for this replay harness"
        )

    parser_id = payload.get("parser_id")
    if not isinstance(parser_id, str) or not parser_id.strip():
        raise ValueError("snapshot.parser_id must be a non-empty string")

    generated_at = payload.get("generated_at")
    if not isinstance(generated_at, str):
        raise ValueError("snapshot.generated_at must be an ISO-8601 string")

    documents_raw = payload.get("documents")
    if not isinstance(documents_raw, list):
        raise ValueError("snapshot.documents must be a list")

    documents: list[SnapshotDocument] = []
    seen_document_ids: set[str] = set()
    for index, row in enumerate(documents_raw):
        location = f"snapshot.documents[{index}]"
        if not isinstance(row, dict):
            raise ValueError(f"{location} must be an object")
        document_id = row.get("document_id")
        if not isinstance(document_id, str) or not document_id.strip():
            raise ValueError(f"{location}.document_id must be a non-empty string")
        if document_id in seen_document_ids:
            raise ValueError(f"snapshot.documents contains duplicate document_id '{document_id}'")
        seen_document_ids.add(document_id)
        fields_raw = row.get("fields")
        if not isinstance(fields_raw, dict):
            raise ValueError(f"{location}.fields must be an object")
        fields: dict[str, FieldPayload] = {}
        for field_name, field_payload in fields_raw.items():
            if not isinstance(field_name, str):
                raise ValueError(f"{location}.fields keys must be strings")
            fields[field_name] = _validate_field_payload(
                field_payload, location=f"{location}.fields['{field_name}']"
            )
        documents.append({"document_id": document_id, "fields": fields})

    snapshot: ReplaySnapshot = {
        "artifact_type": REPLAY_BASELINE_ARTIFACT_TYPE,
        "schema_version": SUPPORTED_ARTIFACT_SCHEMA_VERSION,
        "baseline_version": SUPPORTED_BASELINE_VERSION,
        "parser_id": parser_id,
        "generated_at": generated_at,
        "documents": documents,
    }
    if "evaluation" in payload:
        snapshot["evaluation"] = _evaluation_metadata(payload, name="snapshot")
    if "correction_provenance" in payload:
        correction_provenance = payload["correction_provenance"]
        if not isinstance(correction_provenance, list) or not all(
            isinstance(row, dict) for row in correction_provenance
        ):
            raise ValueError("snapshot.correction_provenance must be a list of objects")
        snapshot["correction_provenance"] = correction_provenance
    return snapshot


def load_snapshot(path: Path) -> ReplaySnapshot:
    """Load and validate snapshot from disk."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    return _validate_snapshot(payload)


def _index_snapshot(snapshot: ReplaySnapshot) -> dict[str, dict[str, FieldPayload]]:
    indexed: dict[str, dict[str, FieldPayload]] = {}
    for row in snapshot["documents"]:
        doc_id = row["document_id"]
        if doc_id in indexed:
            raise ValueError(f"snapshot contains duplicate document_id '{doc_id}'")
        indexed[doc_id] = row["fields"]
    return indexed


def _evaluation_metadata(snapshot: Mapping[str, object], *, name: str) -> EvaluationMetadata:
    raw = snapshot.get("evaluation")
    if not isinstance(raw, Mapping):
        raise ValueError(f"{name} snapshot is missing evaluation metadata")
    corpus_id = raw.get("corpus_id")
    corpus_schema_version = raw.get("corpus_schema_version")
    thresholds = raw.get("thresholds")
    if not isinstance(corpus_id, str) or not corpus_id.strip():
        raise ValueError(f"{name} evaluation.corpus_id must be a non-empty string")
    if (
        isinstance(corpus_schema_version, bool)
        or not isinstance(corpus_schema_version, int)
        or corpus_schema_version < 1
    ):
        raise ValueError(f"{name} evaluation.corpus_schema_version must be a positive integer")
    if not isinstance(thresholds, Mapping):
        raise ValueError(f"{name} evaluation.thresholds must be an object")
    normalized_thresholds: dict[str, float] = {}
    for metric in _METRIC_KEYS:
        value = thresholds.get(metric)
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not 0 <= value <= 1
        ):
            raise ValueError(f"{name} evaluation.thresholds.{metric} must be a number from 0 to 1")
        normalized_thresholds[metric] = float(value)
    return {
        "corpus_id": corpus_id,
        "corpus_schema_version": corpus_schema_version,
        "thresholds": normalized_thresholds,
    }


def evaluate_extraction_accuracy(
    *, baseline: ReplaySnapshot, current: ReplaySnapshot
) -> ExtractionAccuracyReport:
    """Measure comparable field/document accuracy and enforce corpus identity.

    A ``None`` expected value is intentionally unscorable: it is retained in
    the report, but neither rewards nor penalizes the parser until a reviewer
    promotes a concrete golden value.
    """
    baseline_metadata = _evaluation_metadata(baseline, name="baseline")
    current_metadata = _evaluation_metadata(current, name="current")
    for key in ("corpus_id", "corpus_schema_version"):
        if baseline_metadata[key] != current_metadata[key]:
            raise ValueError(f"cannot compare snapshots with mismatched evaluation {key}")

    baseline_index = _index_snapshot(baseline)
    current_index = _index_snapshot(current)
    baseline_document_ids = set(baseline_index)
    current_document_ids = set(current_index)
    if baseline_document_ids != current_document_ids:
        raise ValueError("cannot compare snapshots with mismatched document identities")
    exact = incorrect = missing = extra = unscorable = 0
    passed_documents = 0
    worst: list[dict[str, str]] = []

    for document_id in sorted(baseline_document_ids):
        expected_fields = baseline_index.get(document_id, {})
        actual_fields = current_index.get(document_id, {})
        document_failed = False
        for field_name in sorted(set(expected_fields) | set(actual_fields)):
            expected = expected_fields.get(field_name)
            actual = actual_fields.get(field_name)
            if expected is None:
                extra += 1
                document_failed = True
                worst.append({"document_id": document_id, "field": field_name, "kind": "extra"})
            elif expected["value"] is None:
                unscorable += 1
            elif actual is None:
                missing += 1
                document_failed = True
                worst.append({"document_id": document_id, "field": field_name, "kind": "missing"})
            elif actual["value"] != expected["value"]:
                incorrect += 1
                document_failed = True
                worst.append({"document_id": document_id, "field": field_name, "kind": "incorrect"})
            else:
                exact += 1
        if not document_failed:
            passed_documents += 1

    scored = exact + incorrect + missing
    predicted = exact + incorrect + extra
    corpus_size = len(baseline_document_ids)
    return {
        "corpus_id": baseline_metadata["corpus_id"],
        "corpus_schema_version": baseline_metadata["corpus_schema_version"],
        "corpus_size": corpus_size,
        "exact_matches": exact,
        "incorrect_fields": incorrect,
        "missing_fields": missing,
        "extra_fields": extra,
        "unscorable_fields": unscorable,
        "field_precision": exact / predicted if predicted else 1.0,
        "field_recall": exact / scored if scored else 1.0,
        "exact_match_accuracy": exact / scored if scored else 1.0,
        "document_pass_rate": passed_documents / corpus_size if corpus_size else 1.0,
        "worst_regressions": worst[:20],
    }


def assert_accuracy_thresholds(
    *, baseline: ReplaySnapshot, report: ExtractionAccuracyReport
) -> None:
    """Reject a replay whose comparable metrics fall below reviewed thresholds."""
    thresholds = _evaluation_metadata(baseline, name="baseline")["thresholds"]
    failures = [
        f"{metric}={report[metric]:.4f} < {threshold:.4f}"
        for metric, threshold in thresholds.items()
        if report[metric] < threshold
    ]
    if failures:
        raise ValueError("extraction accuracy threshold regression: " + ", ".join(failures))


def diff_snapshots(
    *,
    baseline: ReplaySnapshot,
    current: ReplaySnapshot,
    expected_change_fields: set[tuple[str, str]] | None = None,
) -> DiffReport:
    """Compare snapshots and classify drift as expected or unexpected."""
    baseline_index = _index_snapshot(baseline)
    current_index = _index_snapshot(current)
    expected = expected_change_fields or set()
    changes: list[FieldDiff] = []

    for document_id in sorted(set(baseline_index) | set(current_index)):
        baseline_fields = baseline_index.get(document_id, {})
        current_fields = current_index.get(document_id, {})
        for field_name in sorted(set(baseline_fields) | set(current_fields)):
            baseline_payload = baseline_fields.get(field_name)
            current_payload = current_fields.get(field_name)
            classification: Literal["expected_change", "unexpected_drift"] = (
                "expected_change" if (document_id, field_name) in expected else "unexpected_drift"
            )

            if baseline_payload is None or current_payload is None:
                changes.append(
                    {
                        "document_id": document_id,
                        "field": field_name,
                        "attribute": "field_presence",
                        "baseline": baseline_payload is not None,
                        "current": current_payload is not None,
                        "classification": classification,
                    }
                )
                continue

            for attribute in ("value", "confidence", "evidence"):
                baseline_value = baseline_payload.get(attribute)
                current_value = current_payload.get(attribute)
                if baseline_value != current_value:
                    changes.append(
                        {
                            "document_id": document_id,
                            "field": field_name,
                            "attribute": attribute,
                            "baseline": baseline_value,
                            "current": current_value,
                            "classification": classification,
                        }
                    )

    expected_changes = sum(1 for item in changes if item["classification"] == "expected_change")
    unexpected_changes = sum(1 for item in changes if item["classification"] == "unexpected_drift")
    return {
        "total_changes": len(changes),
        "expected_changes": expected_changes,
        "unexpected_changes": unexpected_changes,
        "changes": changes,
    }
