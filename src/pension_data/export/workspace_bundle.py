"""Build deterministic generated workspace bundles from persisted extraction rows."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from pension_data.sources.schema import SOURCE_AUTHORITY_TIERS

WORKSPACE_CONTRACT_VERSION = "1.0.0"
GENERATED_DATA_ORIGIN = "generated"

_REQUIRED_TOP_LEVEL_FIELDS = frozenset({"contractVersion", "data_origin", "datasets"})
_REQUIRED_DATASET_FIELDS = frozenset(
    {"id", "name", "domain", "kind", "freshness", "lastUpdated", "rows"}
)
_REQUIRED_ROW_FIELDS = frozenset(
    {
        "confidence",
        "entity",
        "metric",
        "metric_family",
        "plan_period",
        "provenance",
        "review_context",
        "value",
    }
)
_REQUIRED_REVIEW_CONTEXT_FIELDS = frozenset(
    {
        "actionable_flags",
        "escalation_required",
        "source_authority_status",
        "source_authority_tier",
    }
)
_ALLOWED_AUTHORITY_STATUSES = frozenset({"declared", "unknown"})


class WorkspaceBundleError(ValueError):
    """Raised when generated workspace-bundle data violates the producer contract."""


def default_review_context() -> dict[str, Any]:
    """Return explicit unknown review context values for legacy artifacts."""
    return {
        "actionable_flags": [],
        "escalation_required": None,
        "source_authority_status": "unknown",
        "source_authority_tier": None,
    }


def normalize_review_context(
    value: Mapping[str, Any] | None,
    *,
    require_all_fields: bool = False,
) -> dict[str, Any]:
    """Validate and normalize per-run/document review context."""
    if value is None:
        if require_all_fields:
            raise WorkspaceBundleError("workspace review_context must be an object")
        return default_review_context()
    if not isinstance(value, Mapping):
        raise WorkspaceBundleError("workspace review_context must be an object")
    if require_all_fields:
        missing = _REQUIRED_REVIEW_CONTEXT_FIELDS.difference(value)
        if missing:
            raise WorkspaceBundleError(
                "workspace review_context missing required fields: " + ", ".join(sorted(missing))
            )

    escalation_required = value.get("escalation_required")
    if escalation_required is not None and not isinstance(escalation_required, bool):
        raise WorkspaceBundleError(
            "workspace review_context escalation_required must be boolean or null"
        )

    raw_flags = value.get("actionable_flags")
    if raw_flags is None:
        if require_all_fields:
            raise WorkspaceBundleError(
                "workspace review_context actionable_flags must be a list of strings"
            )
        flags: list[str] = []
    elif isinstance(raw_flags, Sequence) and not isinstance(raw_flags, (str, bytes, bytearray)):
        if require_all_fields and not isinstance(raw_flags, list):
            raise WorkspaceBundleError(
                "workspace review_context actionable_flags must be a list of strings"
            )
        flags = []
        for item in raw_flags:
            if not isinstance(item, str) or not item.strip():
                raise WorkspaceBundleError(
                    "workspace review_context actionable_flags must contain non-empty strings"
                )
            token = item.strip()
            if token not in flags:
                flags.append(token)
    else:
        raise WorkspaceBundleError(
            "workspace review_context actionable_flags must be a list of strings"
        )

    raw_tier = value.get("source_authority_tier")
    if raw_tier is None:
        tier: str | None = None
    elif isinstance(raw_tier, str) and raw_tier.strip() in SOURCE_AUTHORITY_TIERS:
        tier = raw_tier.strip()
    else:
        allowed = ", ".join(SOURCE_AUTHORITY_TIERS)
        raise WorkspaceBundleError(
            f"workspace review_context source_authority_tier must be one of [{allowed}] or null"
        )

    expected_status = "declared" if tier is not None else "unknown"
    raw_status = value.get("source_authority_status")
    if raw_status is None:
        if require_all_fields:
            raise WorkspaceBundleError(
                "workspace review_context source_authority_status must be 'declared' or 'unknown'"
            )
        status = expected_status
    elif isinstance(raw_status, str) and raw_status.strip() in _ALLOWED_AUTHORITY_STATUSES:
        status = raw_status.strip()
        if status != expected_status:
            raise WorkspaceBundleError(
                f"workspace review_context source_authority_status='{status}' "
                f"conflicts with source_authority_tier={tier!r}"
            )
    else:
        raise WorkspaceBundleError(
            "workspace review_context source_authority_status must be 'declared' or 'unknown'"
        )

    return {
        "actionable_flags": flags,
        "escalation_required": escalation_required,
        "source_authority_status": status,
        "source_authority_tier": tier,
    }


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


def map_staging_row_to_ui_row(
    row: Mapping[str, Any],
    *,
    review_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Convert one staged core-metric row into the renderer-shell row contract."""
    value = row.get("normalized_value")
    if value in (None, ""):
        value = row.get("as_reported_value")
    if isinstance(value, float) and not math.isfinite(value):
        raise WorkspaceBundleError("workspace metric value must be finite")
    raw_row_context = row.get("review_context")
    resolved_context = normalize_review_context(
        review_context if review_context is not None else raw_row_context
    )
    if (
        review_context is not None
        and raw_row_context is not None
        and normalize_review_context(raw_row_context) != resolved_context
    ):
        raise WorkspaceBundleError("staging row review_context conflicts with run review_context")
    return {
        "confidence": _as_number(row.get("confidence")),
        "entity": _as_text(row.get("plan_id")) or "unknown-plan",
        "metric": _as_text(row.get("metric_name")),
        "metric_family": _as_text(row.get("metric_family")) or "core_metric",
        "plan_period": _as_text(row.get("plan_period")),
        "provenance": {
            "evidence_refs": _evidence_refs(row.get("evidence_refs")),
            "source_document": _as_text(row.get("source_document_id")),
        },
        "review_context": resolved_context,
        "value": value,
    }


def build_workspace_bundle(
    rows: Sequence[Mapping[str, Any]],
    *,
    run_id: str,
    last_updated: str,
    review_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a generated bundle for one extraction run.

    Deliberate-break anchor: omitting ``review_context=resolved_review_context``
    when mapping rows causes both direct and rebuilt bundles to fall back to
    unknown review_context even when ``run_manifest.json`` retains it.
    """
    if not rows:
        raise WorkspaceBundleError("workspace bundle requires at least one staging row")
    normalized_run_id = run_id.strip()
    if not normalized_run_id:
        raise WorkspaceBundleError("workspace bundle run_id is required")

    resolved_review_context = (
        normalize_review_context(review_context) if review_context is not None else None
    )
    ui_rows = [
        map_staging_row_to_ui_row(row, review_context=resolved_review_context) for row in rows
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
        rows = dataset.get("rows")
        if not isinstance(rows, list) or not rows:
            raise WorkspaceBundleError(f"workspace dataset {index} rows must be non-empty")
        for row_index, row in enumerate(rows):
            if not isinstance(row, Mapping):
                raise WorkspaceBundleError(
                    f"workspace dataset {index} row {row_index} must be an object"
                )
            missing_row = _REQUIRED_ROW_FIELDS.difference(row)
            if missing_row:
                raise WorkspaceBundleError(
                    f"workspace dataset {index} row {row_index} missing required fields: "
                    + ", ".join(sorted(missing_row))
                )
            review_context = row.get("review_context")
            normalized_context = normalize_review_context(review_context, require_all_fields=True)
            if review_context != normalized_context:
                raise WorkspaceBundleError("workspace review_context must use canonical values")

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
