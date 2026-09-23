"""Load query-ready API data from completed one-PDF pilot artifacts."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from pension_data.extract.common.evidence import (
    build_evidence_reference,
    canonicalize_evidence_ref,
)
from pension_data.query.metric_history_service import (
    MetricHistoryProvenanceRef,
    MetricHistoryRow,
)
from pension_data.query.saved_views.models import FundingTrendInput


class QueryArtifactError(ValueError):
    """Raised when configured pilot artifacts are absent, unsafe, or invalid."""


@dataclass(frozen=True, slots=True)
class QueryDataSnapshot:
    """Immutable query data loaded from completed pilot runs."""

    metric_history_rows: tuple[MetricHistoryRow, ...]
    funding_trend_inputs: tuple[FundingTrendInput, ...]


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise QueryArtifactError(f"invalid query artifact: {path.name}") from exc


def _contained_existing_path(root: Path, candidate: Path, *, kind: str) -> Path:
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise QueryArtifactError(f"missing query artifact {kind}") from exc
    if not resolved.is_relative_to(root):
        raise QueryArtifactError(f"query artifact {kind} escapes configured root")
    return resolved


def _required_mapping(value: object, *, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise QueryArtifactError(f"query artifact {field} must be an object")
    return value


def _required_text(row: Mapping[str, object], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        raise QueryArtifactError(f"query artifact row requires non-empty {field}")
    return value.strip()


def _optional_text(row: Mapping[str, object], field: str) -> str | None:
    value = row.get(field)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise QueryArtifactError(f"query artifact row {field} must be non-empty or null")
    return value.strip()


def _optional_finite_number(row: Mapping[str, object], field: str) -> float | None:
    value = row.get(field)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise QueryArtifactError(f"query artifact row {field} must be numeric or null")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise QueryArtifactError(f"query artifact row {field} must be finite")
    return normalized


def _required_confidence(row: Mapping[str, object]) -> float:
    confidence = _optional_finite_number(row, "confidence")
    if confidence is None or not 0.0 <= confidence <= 1.0:
        raise QueryArtifactError("query artifact row confidence must be between 0 and 1")
    return confidence


def _validate_temporal(value: str, *, field: str) -> str:
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise QueryArtifactError(f"query artifact row {field} must be ISO-8601") from exc
    return value


def _evidence_refs(row: Mapping[str, object]) -> tuple[str, ...]:
    value = row.get("evidence_refs")
    if not isinstance(value, list) or not value:
        raise QueryArtifactError("query artifact row requires non-empty evidence_refs")
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise QueryArtifactError("query artifact evidence_refs must contain strings")
        token = canonicalize_evidence_ref(item)
        if not token:
            raise QueryArtifactError("query artifact evidence_refs must be non-empty")
        if token not in normalized:
            normalized.append(token)
    return tuple(normalized)


def _provenance_refs(
    *, source_document_id: str, evidence_refs: Sequence[str]
) -> tuple[MetricHistoryProvenanceRef, ...]:
    refs: dict[str, MetricHistoryProvenanceRef] = {}
    for raw_ref in evidence_refs:
        evidence = build_evidence_reference(
            report_id=source_document_id,
            source_document_id=source_document_id,
            evidence_ref=raw_ref,
        )
        refs[evidence.evidence_ref_id] = MetricHistoryProvenanceRef(
            evidence_ref_id=evidence.evidence_ref_id,
            raw_ref=evidence.raw_ref,
            page_number=evidence.page_number,
            section_hint=evidence.section_hint,
            snippet_anchor=evidence.snippet_anchor,
        )
    return tuple(sorted(refs.values(), key=lambda item: item.evidence_ref_id))


def _metric_history_row(row: Mapping[str, object]) -> MetricHistoryRow:
    source_document_id = _required_text(row, "source_document_id")
    as_reported_value = _optional_finite_number(row, "as_reported_value")
    normalized_value = _optional_finite_number(row, "normalized_value")
    if as_reported_value is None and normalized_value is None:
        raise QueryArtifactError("query artifact metric row requires a numeric value")

    effective_date = _validate_temporal(
        _required_text(row, "effective_date"), field="effective_date"
    )
    ingestion_date = _validate_temporal(
        _required_text(row, "ingestion_date"), field="ingestion_date"
    )
    valid_from = _optional_text(row, "valid_from") or effective_date
    asserted_at = _optional_text(row, "asserted_at") or ingestion_date
    _validate_temporal(valid_from, field="valid_from")
    _validate_temporal(asserted_at, field="asserted_at")
    valid_to = _optional_text(row, "valid_to")
    superseded_at = _optional_text(row, "superseded_at")
    if valid_to is not None:
        _validate_temporal(valid_to, field="valid_to")
    if superseded_at is not None:
        _validate_temporal(superseded_at, field="superseded_at")

    restated = row.get("restated", False)
    if not isinstance(restated, bool):
        raise QueryArtifactError("query artifact row restated must be boolean")
    refs = _evidence_refs(row)

    return MetricHistoryRow(
        entity_id=_required_text(row, "plan_id"),
        plan_period=_required_text(row, "plan_period"),
        metric_family=_required_text(row, "metric_family"),
        metric_name=_required_text(row, "metric_name"),
        as_reported_value=as_reported_value,
        normalized_value=normalized_value,
        as_reported_unit=_optional_text(row, "as_reported_unit"),
        normalized_unit=_optional_text(row, "normalized_unit"),
        confidence=_required_confidence(row),
        effective_date=effective_date,
        ingestion_date=ingestion_date,
        benchmark_version=_required_text(row, "benchmark_version"),
        # Pilot staging has one source-document identity and no separate report identity.
        report_id=source_document_id,
        source_document_id=source_document_id,
        provenance_refs=_provenance_refs(
            source_document_id=source_document_id,
            evidence_refs=refs,
        ),
        valid_from=valid_from,
        asserted_at=asserted_at,
        valid_to=valid_to,
        superseded_at=superseded_at,
        is_restated=restated,
    )


def _validate_manifest_row(
    row: Mapping[str, object], *, manifest_input: Mapping[str, object]
) -> None:
    for field in ("plan_id", "plan_period", "source_document_id"):
        manifest_value = manifest_input.get(field)
        if not isinstance(manifest_value, str) or not manifest_value.strip():
            raise QueryArtifactError(f"query run manifest input requires non-empty {field}")
        if _required_text(row, field) != manifest_value.strip():
            raise QueryArtifactError(f"query artifact row {field} conflicts with run manifest")


def _load_run_rows(root: Path, run_dir: Path) -> list[Mapping[str, object]]:
    resolved_run = _contained_existing_path(root, run_dir, kind="run directory")
    manifest_path = _contained_existing_path(
        root, resolved_run / "run_manifest.json", kind="run manifest"
    )
    manifest = _required_mapping(_read_json(manifest_path), field="run_manifest")
    if manifest.get("ledger_status") != "success":
        raise QueryArtifactError("query run manifest is not successful")
    manifest_input = _required_mapping(manifest.get("input"), field="run_manifest.input")

    rows_path = _contained_existing_path(
        root,
        resolved_run / "extraction_persistence" / "staging_core_metrics.json",
        kind="staging_core_metrics.json",
    )
    payload = _read_json(rows_path)
    if not isinstance(payload, list):
        raise QueryArtifactError("staging_core_metrics.json must contain a list")

    rows: list[Mapping[str, object]] = []
    for item in payload:
        row = _required_mapping(item, field="staging_core_metrics row")
        _validate_manifest_row(row, manifest_input=manifest_input)
        rows.append(row)
    return rows


def _funding_trend_inputs(rows: Sequence[MetricHistoryRow]) -> tuple[FundingTrendInput, ...]:
    by_period: dict[tuple[str, str], dict[str, float]] = {}
    metric_names = {
        "funded_ratio": "funded_ratio",
        "employer_contributions": "employer_contributions_usd",
        "employer_contributions_usd": "employer_contributions_usd",
        "employee_contributions": "employee_contributions_usd",
        "employee_contributions_usd": "employee_contributions_usd",
        "benefit_payments": "benefit_payments_usd",
        "benefit_payments_usd": "benefit_payments_usd",
    }
    for row in rows:
        target = metric_names.get(row.metric_name.casefold())
        if target is None or row.normalized_value is None:
            continue
        if target == "funded_ratio" and row.normalized_unit not in {None, "ratio"}:
            raise QueryArtifactError("funded_ratio must use normalized unit 'ratio'")
        key = (row.entity_id, row.plan_period)
        values = by_period.setdefault(key, {})
        previous = values.get(target)
        if previous is not None and previous != row.normalized_value:
            raise QueryArtifactError(f"ambiguous {target} for {row.entity_id} {row.plan_period}")
        values[target] = row.normalized_value

    inputs: list[FundingTrendInput] = []
    for (plan_id, plan_period), values in sorted(by_period.items()):
        funded_ratio = values.get("funded_ratio")
        if funded_ratio is None:
            continue
        inputs.append(
            FundingTrendInput(
                plan_id=plan_id,
                plan_period=plan_period,
                funded_ratio=funded_ratio,
                employer_contributions_usd=values.get("employer_contributions_usd"),
                employee_contributions_usd=values.get("employee_contributions_usd"),
                benefit_payments_usd=values.get("benefit_payments_usd"),
            )
        )
    return tuple(inputs)


def load_query_data_snapshot(artifact_root: Path) -> QueryDataSnapshot:
    """Load deterministic query data from successful runs beneath ``artifact_root``."""
    try:
        root = artifact_root.expanduser().resolve(strict=True)
    except OSError as exc:
        raise QueryArtifactError("configured query artifact root does not exist") from exc
    if not root.is_dir():
        raise QueryArtifactError("configured query artifact root must be a directory")

    pilot_root = _contained_existing_path(root, root / "one_pdf_pilot", kind="one_pdf_pilot")
    if not pilot_root.is_dir():
        raise QueryArtifactError("query artifact one_pdf_pilot must be a directory")

    run_dirs: list[Path] = []
    for candidate in sorted(path for path in pilot_root.iterdir() if path.is_dir()):
        resolved_run = _contained_existing_path(root, candidate, kind="run directory")
        if (resolved_run / "run_manifest.json").is_file():
            run_dirs.append(candidate)
    if not run_dirs:
        raise QueryArtifactError("no completed one-PDF pilot runs found")

    rows_by_fact_id: dict[str, Mapping[str, object]] = {}
    for run_dir in run_dirs:
        for row in _load_run_rows(root, run_dir):
            fact_id = _required_text(row, "fact_id")
            existing = rows_by_fact_id.get(fact_id)
            if existing is not None and dict(existing) != dict(row):
                raise QueryArtifactError(f"conflicting duplicate fact_id: {fact_id}")
            rows_by_fact_id[fact_id] = row

    metric_rows = tuple(
        sorted(
            (_metric_history_row(row) for row in rows_by_fact_id.values()),
            key=lambda row: (
                row.entity_id,
                row.metric_family,
                row.metric_name,
                row.effective_date,
                row.ingestion_date,
                row.source_document_id,
                row.benchmark_version,
            ),
        )
    )
    if not metric_rows:
        raise QueryArtifactError("completed pilot runs contain no query metric rows")
    return QueryDataSnapshot(
        metric_history_rows=metric_rows,
        funding_trend_inputs=_funding_trend_inputs(metric_rows),
    )
