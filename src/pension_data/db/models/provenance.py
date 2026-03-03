"""Evidence/provenance models for page-level metric linkage."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class EvidenceReference:
    """Canonical evidence reference with page or anchor metadata."""

    evidence_ref_id: str
    report_id: str
    source_document_id: str
    raw_ref: str
    page_number: int | None
    section_hint: str | None
    snippet_anchor: str | None


@dataclass(frozen=True, slots=True)
class MetricEvidenceLink:
    """Stable link from one core metric row to one evidence reference."""

    link_id: str
    metric_row_id: str
    metric_family: str
    metric_name: str
    evidence_ref_id: str
