"""Citation-ready export builders for SQL and metric-history payloads."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

CitationStatus = str
SCHEMA_VERSION = "v1"
_CITATION_STATUS_PRESENT: CitationStatus = "present"
_CITATION_STATUS_MISSING: CitationStatus = "missing"

CITATION_COLUMNS: tuple[str, ...] = (
    "citation_status",
    "citation_report_id",
    "citation_source_document_id",
    "citation_source_url",
    "citation_page_number",
    "citation_evidence_ref",
)
SQL_BASE_COLUMNS: tuple[str, ...] = ("row_id",)
METRIC_HISTORY_BASE_COLUMNS: tuple[str, ...] = (
    "row_id",
    "entity_id",
    "plan_period",
    "metric_family",
    "metric_name",
    "as_reported_value",
    "normalized_value",
    "as_reported_unit",
    "normalized_unit",
    "confidence",
    "effective_date",
    "ingestion_date",
    "benchmark_version",
    "report_id",
    "source_document_id",
)


@dataclass(frozen=True, slots=True)
class CitationReference:
    """Citation/provenance pointer for one exported metric observation."""

    report_id: str
    source_document_id: str
    source_url: str | None
    page_number: int | None
    evidence_ref: str


@dataclass(frozen=True, slots=True)
class MetricHistoryExportInput:
    """Metric-history row payload accepted by export builders."""

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
    provenance_refs: tuple[CitationReference, ...]


@dataclass(frozen=True, slots=True)
class CitationBundleEntry:
    """One unique citation entry in a citation bundle export payload."""

    report_id: str
    source_document_id: str
    source_url: str | None
    page_number: int | None
    evidence_ref: str


@dataclass(frozen=True, slots=True)
class CitationBundle:
    """Citation bundle payload with explicit missing-provenance row IDs."""

    citations: tuple[CitationBundleEntry, ...]
    missing_row_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CitationExport:
    """Tabular citation export payload with stable schema metadata."""

    schema_name: str
    schema_version: str
    field_names: tuple[str, ...]
    rows: tuple[dict[str, Any], ...]
    citation_bundle: CitationBundle


def _normalize_text(value: str) -> str:
    return " ".join(value.strip().split())


def _normalize_source_url(value: str | None) -> str | None:
    if value is None:
        return None
    token = _normalize_text(value)
    return token if token else None


def _normalize_citation(ref: CitationReference) -> CitationReference | None:
    report_id = _normalize_text(ref.report_id)
    source_document_id = _normalize_text(ref.source_document_id)
    evidence_ref = _normalize_text(ref.evidence_ref)
    source_url = _normalize_source_url(ref.source_url)
    if not report_id or not source_document_id or not evidence_ref:
        return None
    return CitationReference(
        report_id=report_id,
        source_document_id=source_document_id,
        source_url=source_url,
        page_number=ref.page_number,
        evidence_ref=evidence_ref,
    )


def _sorted_citations(values: Sequence[CitationReference]) -> tuple[CitationReference, ...]:
    deduped: dict[tuple[str, str, str | None, int | None, str], CitationReference] = {}
    for value in values:
        normalized = _normalize_citation(value)
        if normalized is None:
            continue
        key = (
            normalized.report_id,
            normalized.source_document_id,
            normalized.source_url,
            normalized.page_number,
            normalized.evidence_ref,
        )
        deduped[key] = normalized
    return tuple(
        sorted(
            deduped.values(),
            key=lambda row: (
                row.report_id,
                row.source_document_id,
                row.source_url or "",
                row.page_number if row.page_number is not None else -1,
                row.evidence_ref,
            ),
        )
    )


def _with_citation_columns(
    *,
    data: Mapping[str, Any],
    citation_status: CitationStatus,
    citation: CitationReference | None,
) -> dict[str, Any]:
    row: dict[str, Any] = dict(data)
    row["citation_status"] = citation_status
    row["citation_report_id"] = citation.report_id if citation is not None else None
    row["citation_source_document_id"] = citation.source_document_id if citation is not None else None
    row["citation_source_url"] = citation.source_url if citation is not None else None
    row["citation_page_number"] = citation.page_number if citation is not None else None
    row["citation_evidence_ref"] = citation.evidence_ref if citation is not None else None
    return row


def _citation_bundle_from_rows(rows: Sequence[Mapping[str, Any]]) -> CitationBundle:
    citations_by_key: dict[tuple[str, str, str | None, int | None, str], CitationBundleEntry] = {}
    missing_rows: list[str] = []
    for row in rows:
        status = str(row.get("citation_status", ""))
        row_id = str(row.get("row_id", ""))
        if status == _CITATION_STATUS_MISSING:
            if row_id:
                missing_rows.append(row_id)
            continue
        report_id = row.get("citation_report_id")
        source_document_id = row.get("citation_source_document_id")
        evidence_ref = row.get("citation_evidence_ref")
        if not isinstance(report_id, str) or not isinstance(source_document_id, str):
            continue
        if not isinstance(evidence_ref, str):
            continue
        source_url = row.get("citation_source_url")
        page_number = row.get("citation_page_number")
        normalized_source_url = source_url if isinstance(source_url, str) else None
        normalized_page_number = page_number if isinstance(page_number, int) else None
        key = (
            report_id,
            source_document_id,
            normalized_source_url,
            normalized_page_number,
            evidence_ref,
        )
        citations_by_key[key] = CitationBundleEntry(
            report_id=report_id,
            source_document_id=source_document_id,
            source_url=normalized_source_url,
            page_number=normalized_page_number,
            evidence_ref=evidence_ref,
        )
    return CitationBundle(
        citations=tuple(
            sorted(
                citations_by_key.values(),
                key=lambda row: (
                    row.report_id,
                    row.source_document_id,
                    row.source_url or "",
                    row.page_number if row.page_number is not None else -1,
                    row.evidence_ref,
                ),
            )
        ),
        missing_row_ids=tuple(sorted(set(missing_rows))),
    )


def build_sql_citation_export(
    *,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    provenance_by_row_id: Mapping[str, Sequence[CitationReference]] | None = None,
) -> CitationExport:
    """Build citation-ready tabular export for SQL query rows."""
    normalized_columns = tuple(_normalize_text(column) for column in columns)
    if not normalized_columns:
        raise ValueError("columns must be non-empty")
    if any(not column for column in normalized_columns):
        raise ValueError("columns must not include empty names")
    if len(set(normalized_columns)) != len(normalized_columns):
        raise ValueError("columns must be unique")

    provenance_map = provenance_by_row_id or {}
    exported_rows: list[dict[str, Any]] = []
    for index, values in enumerate(rows, start=1):
        row_id = f"sql:{index}"
        if len(values) != len(normalized_columns):
            raise ValueError("row value count does not match columns")
        base_data: dict[str, Any] = {"row_id": row_id}
        for column, value in zip(normalized_columns, values, strict=True):
            base_data[column] = value

        citations = _sorted_citations(provenance_map.get(row_id, ()))
        if not citations:
            exported_rows.append(
                _with_citation_columns(
                    data=base_data,
                    citation_status=_CITATION_STATUS_MISSING,
                    citation=None,
                )
            )
            continue

        for citation in citations:
            exported_rows.append(
                _with_citation_columns(
                    data=base_data,
                    citation_status=_CITATION_STATUS_PRESENT,
                    citation=citation,
                )
            )

    return CitationExport(
        schema_name="sql_citation_export",
        schema_version=SCHEMA_VERSION,
        field_names=(*SQL_BASE_COLUMNS, *normalized_columns, *CITATION_COLUMNS),
        rows=tuple(exported_rows),
        citation_bundle=_citation_bundle_from_rows(exported_rows),
    )


def build_metric_history_citation_export(
    rows: Sequence[MetricHistoryExportInput],
) -> CitationExport:
    """Build citation-ready tabular export for metric-history result rows."""
    exported_rows: list[dict[str, Any]] = []
    ordered_rows = sorted(
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
    for index, row in enumerate(ordered_rows, start=1):
        row_id = f"metric-history:{index}"
        base_data = {
            "row_id": row_id,
            "entity_id": row.entity_id,
            "plan_period": row.plan_period,
            "metric_family": row.metric_family,
            "metric_name": row.metric_name,
            "as_reported_value": row.as_reported_value,
            "normalized_value": row.normalized_value,
            "as_reported_unit": row.as_reported_unit,
            "normalized_unit": row.normalized_unit,
            "confidence": row.confidence,
            "effective_date": row.effective_date,
            "ingestion_date": row.ingestion_date,
            "benchmark_version": row.benchmark_version,
            "report_id": row.report_id,
            "source_document_id": row.source_document_id,
        }
        citations = _sorted_citations(row.provenance_refs)
        if not citations:
            exported_rows.append(
                _with_citation_columns(
                    data=base_data,
                    citation_status=_CITATION_STATUS_MISSING,
                    citation=None,
                )
            )
            continue

        for citation in citations:
            exported_rows.append(
                _with_citation_columns(
                    data=base_data,
                    citation_status=_CITATION_STATUS_PRESENT,
                    citation=citation,
                )
            )

    return CitationExport(
        schema_name="metric_history_citation_export",
        schema_version=SCHEMA_VERSION,
        field_names=(*METRIC_HISTORY_BASE_COLUMNS, *CITATION_COLUMNS),
        rows=tuple(exported_rows),
        citation_bundle=_citation_bundle_from_rows(exported_rows),
    )
