"""Evidence/provenance models for page-level metric linkage."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

EvidenceMethod = Literal["table", "text", "fallback", "ocr", "llm"]


@dataclass(frozen=True, slots=True)
class EvidenceReference:
    """Canonical evidence reference with page or anchor metadata.

    ``excerpt`` and ``method`` are optional enrichment fields. They carry the
    quoted supporting text and the extraction path that produced the reference.
    Both are deliberately excluded from ``evidence_ref_id`` so adding them never
    changes the stable identity of an existing locator (see
    ``build_evidence_reference``).
    """

    evidence_ref_id: str
    report_id: str
    source_document_id: str
    raw_ref: str
    page_number: int | None
    section_hint: str | None
    snippet_anchor: str | None
    excerpt: str | None = None
    method: EvidenceMethod | None = None


@dataclass(frozen=True, slots=True)
class MetricEvidenceLink:
    """Stable link from one core metric row to one evidence reference.

    ``confidence`` is an optional per-link score. It lets two evidence sources
    for the same fact carry differing confidences, independent of the fact's own
    confidence. It defaults to ``None`` and does not participate in ``link_id``.
    """

    link_id: str
    metric_row_id: str
    metric_family: str
    metric_name: str
    evidence_ref_id: str
    confidence: float | None = None
