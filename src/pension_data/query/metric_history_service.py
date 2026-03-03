"""Metric history query service with bitemporal and provenance-aware filtering."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from pension_data.db.models.core_facts import (
    ActuarialFact,
    AllocationFact,
    FeeFact,
    FundedStatusFact,
    HoldingFact,
)
from pension_data.extract.common.evidence import build_evidence_reference, canonicalize_evidence_ref


@dataclass(frozen=True, slots=True)
class MetricHistoryProvenanceRef:
    """Provenance pointer for one metric-history row evidence reference."""

    evidence_ref_id: str
    raw_ref: str
    page_number: int | None
    section_hint: str | None
    snippet_anchor: str | None


@dataclass(frozen=True, slots=True)
class MetricHistoryRow:
    """Metric-history row with bitemporal and provenance fields."""

    entity_id: str
    plan_period: str
    metric_family: str
    metric_name: str
    as_reported_value: float | None
    normalized_value: float | None
    as_reported_unit: str | None
    normalized_unit: str | None
    confidence: float
    effective_date: str
    ingestion_date: str
    benchmark_version: str
    report_id: str
    source_document_id: str
    provenance_refs: tuple[MetricHistoryProvenanceRef, ...]


@dataclass(frozen=True, slots=True)
class MetricHistoryRequest:
    """Metric-history endpoint contract for entity, metric, and bitemporal filters."""

    entity_id: str
    metric_name: str | None = None
    metric_family: str | None = None
    effective_start: str | None = None
    effective_end: str | None = None
    ingestion_start: str | None = None
    ingestion_end: str | None = None
    limit: int = 500


@dataclass(frozen=True, slots=True)
class MetricHistoryResponse:
    """Metric-history query envelope with total-match metadata."""

    rows: tuple[MetricHistoryRow, ...]
    total_rows: int


def _parse_iso_temporal(value: str, *, field_name: str) -> datetime:
    candidate = value.strip()
    if not candidate:
        raise ValueError(f"{field_name} must be a non-empty ISO-8601 date or datetime string")
    normalized = f"{candidate[:-1]}+00:00" if candidate.endswith("Z") else candidate
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:  # pragma: no cover - defensive parse guard
        raise ValueError(
            f"{field_name} must be an ISO-8601 date or datetime string: {value!r}"
        ) from exc
    if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _normalize_metric_token(value: str | None) -> str | None:
    if value is None:
        return None
    token = value.strip()
    if not token:
        return None
    return token


def _normalize_provenance_refs(
    *,
    report_id: str,
    source_document_id: str,
    evidence_refs: Sequence[str],
) -> tuple[MetricHistoryProvenanceRef, ...]:
    refs_by_id: dict[str, MetricHistoryProvenanceRef] = {}
    for raw_ref in evidence_refs:
        normalized_ref = canonicalize_evidence_ref(raw_ref)
        if not normalized_ref:
            continue
        evidence = build_evidence_reference(
            report_id=report_id,
            source_document_id=source_document_id,
            evidence_ref=normalized_ref,
        )
        refs_by_id[evidence.evidence_ref_id] = MetricHistoryProvenanceRef(
            evidence_ref_id=evidence.evidence_ref_id,
            raw_ref=evidence.raw_ref,
            page_number=evidence.page_number,
            section_hint=evidence.section_hint,
            snippet_anchor=evidence.snippet_anchor,
        )
    return tuple(sorted(refs_by_id.values(), key=lambda row: row.evidence_ref_id))


def build_metric_history_rows(
    *,
    funded_facts: Sequence[FundedStatusFact] = (),
    actuarial_facts: Sequence[ActuarialFact] = (),
    allocation_facts: Sequence[AllocationFact] = (),
    holding_facts: Sequence[HoldingFact] = (),
    fee_facts: Sequence[FeeFact] = (),
) -> list[MetricHistoryRow]:
    """Build deterministic metric-history rows from bitemporal core facts."""
    rows: list[MetricHistoryRow] = []

    for fact in funded_facts:
        rows.append(
            MetricHistoryRow(
                entity_id=fact.context.plan_id,
                plan_period=fact.context.plan_period,
                metric_family="funded",
                metric_name=fact.metric_name,
                as_reported_value=fact.metric_value.as_reported_value,
                normalized_value=fact.metric_value.normalized_value,
                as_reported_unit=fact.metric_value.as_reported_unit,
                normalized_unit=fact.metric_value.normalized_unit,
                confidence=fact.confidence,
                effective_date=fact.context.effective_date,
                ingestion_date=fact.context.ingestion_date,
                benchmark_version=fact.context.benchmark_version,
                report_id=fact.context.source_document_id,
                source_document_id=fact.context.source_document_id,
                provenance_refs=_normalize_provenance_refs(
                    report_id=fact.context.source_document_id,
                    source_document_id=fact.context.source_document_id,
                    evidence_refs=fact.evidence_refs,
                ),
            )
        )

    for fact in actuarial_facts:
        rows.append(
            MetricHistoryRow(
                entity_id=fact.context.plan_id,
                plan_period=fact.context.plan_period,
                metric_family="actuarial",
                metric_name=fact.metric_name,
                as_reported_value=fact.metric_value.as_reported_value,
                normalized_value=fact.metric_value.normalized_value,
                as_reported_unit=fact.metric_value.as_reported_unit,
                normalized_unit=fact.metric_value.normalized_unit,
                confidence=fact.confidence,
                effective_date=fact.context.effective_date,
                ingestion_date=fact.context.ingestion_date,
                benchmark_version=fact.context.benchmark_version,
                report_id=fact.context.source_document_id,
                source_document_id=fact.context.source_document_id,
                provenance_refs=_normalize_provenance_refs(
                    report_id=fact.context.source_document_id,
                    source_document_id=fact.context.source_document_id,
                    evidence_refs=fact.evidence_refs,
                ),
            )
        )

    for fact in allocation_facts:
        rows.append(
            MetricHistoryRow(
                entity_id=fact.context.plan_id,
                plan_period=fact.context.plan_period,
                metric_family="allocation",
                metric_name=fact.metric_name,
                as_reported_value=fact.metric_value.as_reported_value,
                normalized_value=fact.metric_value.normalized_value,
                as_reported_unit=fact.metric_value.as_reported_unit,
                normalized_unit=fact.metric_value.normalized_unit,
                confidence=fact.confidence,
                effective_date=fact.context.effective_date,
                ingestion_date=fact.context.ingestion_date,
                benchmark_version=fact.context.benchmark_version,
                report_id=fact.context.source_document_id,
                source_document_id=fact.context.source_document_id,
                provenance_refs=_normalize_provenance_refs(
                    report_id=fact.context.source_document_id,
                    source_document_id=fact.context.source_document_id,
                    evidence_refs=fact.evidence_refs,
                ),
            )
        )

    for fact in holding_facts:
        rows.append(
            MetricHistoryRow(
                entity_id=fact.context.plan_id,
                plan_period=fact.context.plan_period,
                metric_family="holding",
                metric_name=fact.metric_name,
                as_reported_value=fact.metric_value.as_reported_value,
                normalized_value=fact.metric_value.normalized_value,
                as_reported_unit=fact.metric_value.as_reported_unit,
                normalized_unit=fact.metric_value.normalized_unit,
                confidence=fact.confidence,
                effective_date=fact.context.effective_date,
                ingestion_date=fact.context.ingestion_date,
                benchmark_version=fact.context.benchmark_version,
                report_id=fact.context.source_document_id,
                source_document_id=fact.context.source_document_id,
                provenance_refs=_normalize_provenance_refs(
                    report_id=fact.context.source_document_id,
                    source_document_id=fact.context.source_document_id,
                    evidence_refs=fact.evidence_refs,
                ),
            )
        )

    for fact in fee_facts:
        rows.append(
            MetricHistoryRow(
                entity_id=fact.context.plan_id,
                plan_period=fact.context.plan_period,
                metric_family="fee",
                metric_name=fact.fee_category,
                as_reported_value=fact.metric_value.as_reported_value,
                normalized_value=fact.metric_value.normalized_value,
                as_reported_unit=fact.metric_value.as_reported_unit,
                normalized_unit=fact.metric_value.normalized_unit,
                confidence=fact.confidence,
                effective_date=fact.context.effective_date,
                ingestion_date=fact.context.ingestion_date,
                benchmark_version=fact.context.benchmark_version,
                report_id=fact.context.source_document_id,
                source_document_id=fact.context.source_document_id,
                provenance_refs=_normalize_provenance_refs(
                    report_id=fact.context.source_document_id,
                    source_document_id=fact.context.source_document_id,
                    evidence_refs=fact.evidence_refs,
                ),
            )
        )

    return sorted(
        rows,
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


def query_metric_history(
    rows: Sequence[MetricHistoryRow],
    *,
    request: MetricHistoryRequest,
) -> MetricHistoryResponse:
    """Query metric history rows with bitemporal and metric/entity filters."""
    entity_id = request.entity_id.strip()
    if not entity_id:
        raise ValueError("entity_id is required")
    if request.limit < 1:
        raise ValueError("limit must be >= 1")

    metric_name = _normalize_metric_token(request.metric_name)
    metric_family = _normalize_metric_token(request.metric_family)

    effective_start = (
        _parse_iso_temporal(request.effective_start, field_name="effective_start")
        if request.effective_start is not None
        else None
    )
    effective_end = (
        _parse_iso_temporal(request.effective_end, field_name="effective_end")
        if request.effective_end is not None
        else None
    )
    ingestion_start = (
        _parse_iso_temporal(request.ingestion_start, field_name="ingestion_start")
        if request.ingestion_start is not None
        else None
    )
    ingestion_end = (
        _parse_iso_temporal(request.ingestion_end, field_name="ingestion_end")
        if request.ingestion_end is not None
        else None
    )

    filtered: list[MetricHistoryRow] = []
    for row in rows:
        if row.entity_id != entity_id:
            continue
        if metric_name is not None and row.metric_name != metric_name:
            continue
        if metric_family is not None and row.metric_family != metric_family:
            continue

        row_effective = _parse_iso_temporal(row.effective_date, field_name="row.effective_date")
        row_ingestion = _parse_iso_temporal(row.ingestion_date, field_name="row.ingestion_date")

        if effective_start is not None and row_effective < effective_start:
            continue
        if effective_end is not None and row_effective > effective_end:
            continue
        if ingestion_start is not None and row_ingestion < ingestion_start:
            continue
        if ingestion_end is not None and row_ingestion > ingestion_end:
            continue
        filtered.append(row)

    ordered = sorted(
        filtered,
        key=lambda row: (
            _parse_iso_temporal(row.effective_date, field_name="row.effective_date"),
            _parse_iso_temporal(row.ingestion_date, field_name="row.ingestion_date"),
            row.metric_family,
            row.metric_name,
            row.source_document_id,
            row.benchmark_version,
        ),
    )

    limited = tuple(ordered[: request.limit])
    return MetricHistoryResponse(rows=limited, total_rows=len(ordered))
