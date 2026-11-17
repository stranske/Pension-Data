"""Build deterministic generated workspace bundles from persisted extraction rows."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

WORKSPACE_CONTRACT_VERSION = "1.0.0"
GENERATED_DATA_ORIGIN = "generated"

_REQUIRED_TOP_LEVEL_FIELDS = frozenset({"contractVersion", "data_origin", "datasets"})
_REQUIRED_DATASET_FIELDS = frozenset(
    {"id", "name", "domain", "kind", "freshness", "lastUpdated", "rows"}
)
_REQUIRED_PROVENANCE_FIELDS = frozenset({"evidence_refs", "source_document"})
_OPTIONAL_PROVENANCE_FIELDS = frozenset({"source_locator"})
_ALLOWED_PROVENANCE_FIELDS = _REQUIRED_PROVENANCE_FIELDS | _OPTIONAL_PROVENANCE_FIELDS


class WorkspaceBundleError(ValueError):
    """Raised when generated workspace-bundle data violates the producer contract."""


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _as_number(value: Any, *, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(number):
        raise WorkspaceBundleError("workspace confidence must be finite")
    return number


def _evidence_refs(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        try:
            decoded = json.loads(stripped)
        except json.JSONDecodeError:
            return [stripped]
        return _evidence_refs(decoded)
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [_as_text(item) for item in value if _as_text(item)]
    return [_as_text(value)] if _as_text(value) else []


def _normalize_source_locator(value: Any) -> str:
    locator = _as_text(value)
    if not locator:
        return ""
    if (
        locator.startswith("/")
        or locator.endswith("/")
        or "\\" in locator
        or "\x00" in locator
        or "://" in locator
    ):
        raise WorkspaceBundleError(
            f"workspace source_locator must be a safe relative path: {locator}"
        )
    segments = locator.split("/")
    if any(segment in {"", ".", ".."} or ":" in segment for segment in segments):
        raise WorkspaceBundleError(
            f"workspace source_locator must be a safe relative path: {locator}"
        )
    return locator


def map_staging_row_to_ui_row(
    row: Mapping[str, Any],
    *,
    source_locators: Mapping[str, str] | None = None,
    source_locator: str | None = None,
) -> dict[str, Any]:
    """Convert one staged core-metric row into the renderer-shell row contract."""
    value = row.get("normalized_value")
    if value in (None, ""):
        value = row.get("as_reported_value")
    if isinstance(value, float) and not math.isfinite(value):
        raise WorkspaceBundleError("workspace metric value must be finite")
    source_document = _as_text(row.get("source_document_id"))
    raw_locator: Any = row.get("source_locator")
    if not _as_text(raw_locator) and source_locators is not None and source_document:
        raw_locator = source_locators.get(source_document)
    if not _as_text(raw_locator) and source_locator is not None:
        raw_locator = source_locator
    resolved_locator = _normalize_source_locator(raw_locator)
    provenance: dict[str, Any] = {
        "evidence_refs": _evidence_refs(row.get("evidence_refs")),
        "source_document": source_document,
    }
    if resolved_locator:
        provenance["source_locator"] = resolved_locator
    return {
        "confidence": _as_number(row.get("confidence")),
        "entity": _as_text(row.get("plan_id")) or "unknown-plan",
        "metric": _as_text(row.get("metric_name")),
        "metric_family": _as_text(row.get("metric_family")) or "core_metric",
        "plan_period": _as_text(row.get("plan_period")),
        "provenance": provenance,
        "value": value,
    }


def build_workspace_bundle(
    rows: Sequence[Mapping[str, Any]],
    *,
    run_id: str,
    last_updated: str,
    source_locators: Mapping[str, str] | None = None,
    source_locator: str | None = None,
) -> dict[str, Any]:
    """Build a generated bundle for one extraction run."""
    if not rows:
        raise WorkspaceBundleError("workspace bundle requires at least one staging row")
    normalized_run_id = run_id.strip()
    if not normalized_run_id:
        raise WorkspaceBundleError("workspace bundle run_id is required")

    ui_rows = [
        map_staging_row_to_ui_row(
            row,
            source_locators=source_locators,
            source_locator=source_locator,
        )
        for row in rows
    ]
    ui_rows.sort(
        key=lambda row: (
            _as_text(row["entity"]),
            _as_text(row["plan_period"]),
            _as_text(row["metric_family"]),
            _as_text(row["metric"]),
            json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False),
        )
    )
    bundle = {
        "contractVersion": WORKSPACE_CONTRACT_VERSION,
        "data_origin": GENERATED_DATA_ORIGIN,
        "datasets": [
            {
                "domain": "pension",
                "freshness": "generated",
                "id": f"one-pdf-pilot-{normalized_run_id}",
                "kind": "core_metrics",
                "lastUpdated": last_updated.strip(),
                "name": f"One-PDF Pilot Generated Metrics ({normalized_run_id})",
                "rows": ui_rows,
            }
        ],
    }
    validate_generated_workspace_bundle(bundle)
    return bundle


def validate_generated_workspace_bundle(payload: Mapping[str, Any]) -> None:
    """Validate the package producer's narrower generated-bundle guarantees."""
    missing_top_level = _REQUIRED_TOP_LEVEL_FIELDS.difference(payload)
    if missing_top_level:
        raise WorkspaceBundleError(
            "workspace bundle missing required fields: " + ", ".join(sorted(missing_top_level))
        )
    if payload.get("contractVersion") != WORKSPACE_CONTRACT_VERSION:
        raise WorkspaceBundleError(
            f"workspace contractVersion must be {WORKSPACE_CONTRACT_VERSION}"
        )
    if payload.get("data_origin") != GENERATED_DATA_ORIGIN:
        raise WorkspaceBundleError("generated workspace bundle requires data_origin='generated'")

    datasets = payload.get("datasets")
    if not isinstance(datasets, list) or not datasets:
        raise WorkspaceBundleError("workspace dataset inventory must be a non-empty list")
    for index, dataset in enumerate(datasets):
        if not isinstance(dataset, Mapping):
            raise WorkspaceBundleError(f"workspace dataset {index} must be an object")
        missing_dataset = _REQUIRED_DATASET_FIELDS.difference(dataset)
        if missing_dataset:
            raise WorkspaceBundleError(
                f"workspace dataset {index} missing required fields: "
                + ", ".join(sorted(missing_dataset))
            )
        if not isinstance(dataset.get("rows"), list) or not dataset["rows"]:
            raise WorkspaceBundleError(f"workspace dataset {index} rows must be non-empty")
        for row_index, row in enumerate(dataset["rows"]):
            if not isinstance(row, Mapping):
                raise WorkspaceBundleError(
                    f"workspace dataset {index} row {row_index} must be an object"
                )
            provenance = row.get("provenance")
            if not isinstance(provenance, Mapping):
                raise WorkspaceBundleError(
                    f"workspace dataset {index} row {row_index} provenance must be an object"
                )
            missing_provenance = _REQUIRED_PROVENANCE_FIELDS.difference(provenance)
            if missing_provenance:
                raise WorkspaceBundleError(
                    f"workspace dataset {index} row {row_index} provenance missing required fields: "
                    + ", ".join(sorted(missing_provenance))
                )
            unknown_provenance = set(provenance).difference(_ALLOWED_PROVENANCE_FIELDS)
            if unknown_provenance:
                raise WorkspaceBundleError(
                    f"workspace dataset {index} row {row_index} provenance has unsupported fields: "
                    + ", ".join(sorted(unknown_provenance))
                )
            if not isinstance(provenance.get("source_document"), str) or not _as_text(
                provenance.get("source_document")
            ):
                raise WorkspaceBundleError(
                    f"workspace dataset {index} row {row_index} provenance.source_document is required"
                )
            evidence_refs = provenance.get("evidence_refs")
            if not isinstance(evidence_refs, list) or any(
                not isinstance(ref, str) or not ref.strip() for ref in evidence_refs
            ):
                raise WorkspaceBundleError(
                    f"workspace dataset {index} row {row_index} provenance.evidence_refs must be a list of strings"
                )
            if "source_locator" in provenance:
                raw_locator = provenance.get("source_locator")
                if not isinstance(raw_locator, str) or not _normalize_source_locator(raw_locator):
                    raise WorkspaceBundleError(
                        f"workspace dataset {index} row {row_index} provenance.source_locator must be a non-empty relative path"
                    )

    try:
        json.dumps(payload, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise WorkspaceBundleError(f"workspace bundle is not strict JSON: {exc}") from exc


def write_workspace_bundle(path: Path, payload: Mapping[str, Any]) -> Path:
    """Validate and atomically-shaped serialize a generated workspace bundle."""
    validate_generated_workspace_bundle(payload)
    serialized = json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(serialized, encoding="utf-8")
    return path
