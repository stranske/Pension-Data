"""Replay harness utilities for golden-corpus regression detection."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, TypedDict

SUPPORTED_BASELINE_VERSION = "v1"


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

    baseline_version: str
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
    unexpected_changes: int
    changes: list[FieldDiff]


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
    generated_at: datetime | None = None,
) -> ReplaySnapshot:
    """Build JSON-friendly replay snapshot from run output."""
    timestamp = (generated_at or datetime.now(UTC)).astimezone(UTC).isoformat()
    ordered_results = sorted(replay_results, key=lambda item: item.document_id)
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
        "baseline_version": baseline_version,
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
    if payload.get("baseline_version") != SUPPORTED_BASELINE_VERSION:
        raise ValueError(
            "snapshot baseline_version must equal "
            f"'{SUPPORTED_BASELINE_VERSION}' for this replay harness"
        )

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

    return {
        "baseline_version": SUPPORTED_BASELINE_VERSION,
        "generated_at": generated_at,
        "documents": documents,
    }


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

    unexpected_changes = sum(1 for item in changes if item["classification"] == "unexpected_drift")
    return {
        "total_changes": len(changes),
        "unexpected_changes": unexpected_changes,
        "changes": changes,
    }
