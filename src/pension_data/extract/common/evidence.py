"""Evidence-reference parsing and canonicalization helpers."""

from __future__ import annotations

import re

from stranske_pdf_extract.contract import EvidenceRef, ExtractionMethod

from pension_data.db.models.provenance import EvidenceMethod, EvidenceReference
from pension_data.extract.common.ids import stable_id

_PAGE_REF_PATTERN = re.compile(
    r"^(?:p(?:age)?[\s\.:#-]*)(?P<page>\d+)(?:\s*(?:#|:|-)\s*(?P<section>.+))?$",
    re.IGNORECASE,
)


def canonicalize_evidence_ref(raw_ref: str) -> str:
    """Normalize free-form evidence refs to deterministic tokens."""
    token = raw_ref.strip()
    if not token:
        return ""

    lowered = token.lower()
    if lowered.startswith("text:"):
        suffix = lowered.split(":", 1)[1].strip()
        return f"text:{suffix}" if suffix else "text:unknown"

    page_match = _PAGE_REF_PATTERN.match(token)
    if page_match is not None:
        page = int(page_match.group("page"))
        section = (page_match.group("section") or "").strip()
        if section:
            collapsed_section = " ".join(section.split())
            return f"p.{page}#{collapsed_section}"
        return f"p.{page}"

    return " ".join(token.split())


def text_block_evidence_ref(block_index: int) -> str:
    """Build a deterministic text-block evidence anchor."""
    return f"text:{block_index + 1}"


def table_evidence_ref(raw_ref: str | None) -> str:
    """Return canonical table evidence reference with a deterministic fallback."""
    if raw_ref is None:
        return "table:unknown"
    normalized = canonicalize_evidence_ref(raw_ref)
    if not normalized:
        return "table:unknown"
    if normalized.startswith(("p.", "text:", "table:")):
        return normalized
    return f"table:{normalized}"


def _section_implies_table(section_hint: str | None) -> bool:
    if section_hint is None:
        return False
    normalized = section_hint.strip().lower()
    return normalized == "table" or normalized.startswith("table ")


def _method_from_ref(
    *,
    normalized_ref: str,
    page_number: int | None,
    section_hint: str | None,
) -> EvidenceMethod | None:
    """Infer the extraction method from the canonical anchor form.

    ``table:`` anchors come from the table-extraction path, ``text:`` anchors and
    page locators from the text path. Free-form section hints leave the method
    unset so callers can override with a more precise value.
    """
    if normalized_ref.startswith("table:"):
        return "table"
    if normalized_ref.startswith("text:"):
        return "text"
    if _section_implies_table(section_hint):
        return "table"
    if page_number is not None:
        return "text"
    return None


def build_evidence_reference(
    *,
    report_id: str,
    source_document_id: str,
    evidence_ref: str,
    excerpt: str | None = None,
    method: EvidenceMethod | None = None,
) -> EvidenceReference:
    """Parse one evidence ref into structured page/anchor fields.

    ``excerpt`` (the quoted supporting text) and ``method`` (the extraction path)
    are optional enrichment. When ``method`` is not supplied it is inferred from
    the canonical anchor form. Neither field participates in ``evidence_ref_id``,
    so enriching an existing locator never changes its stable identity.
    """
    normalized_ref = canonicalize_evidence_ref(evidence_ref)
    if not normalized_ref:
        raise ValueError("evidence_ref must be non-empty")

    page_number: int | None = None
    section_hint: str | None = None
    snippet_anchor: str | None = None

    if normalized_ref.startswith("text:"):
        snippet_anchor = normalized_ref
    else:
        page_match = re.match(r"^p\.(\d+)(?:#(.+))?$", normalized_ref, re.IGNORECASE)
        if page_match is not None:
            page_number = int(page_match.group(1))
            section_hint = page_match.group(2).strip() if page_match.group(2) else None
        elif normalized_ref.startswith("table:"):
            snippet_anchor = normalized_ref
        else:
            section_hint = normalized_ref

    resolved_method = method or _method_from_ref(
        normalized_ref=normalized_ref,
        page_number=page_number,
        section_hint=section_hint,
    )
    resolved_excerpt = excerpt.strip() if excerpt and excerpt.strip() else None
    shared_ref = EvidenceRef(
        source_doc_id=source_document_id,
        page_number=page_number,
        section_hint=section_hint,
        snippet_anchor=snippet_anchor,
        excerpt=resolved_excerpt,
        method=resolved_method,
    )

    return EvidenceReference(
        evidence_ref_id=stable_id(
            "evidence",
            report_id,
            shared_ref.source_doc_id,
            normalized_ref,
            shared_ref.page_number,
            shared_ref.section_hint,
            shared_ref.snippet_anchor,
        ),
        report_id=report_id,
        source_document_id=shared_ref.source_doc_id,
        raw_ref=normalized_ref,
        page_number=shared_ref.page_number,
        section_hint=shared_ref.section_hint,
        snippet_anchor=shared_ref.snippet_anchor,
        excerpt=shared_ref.excerpt,
        method=shared_ref.method,
    )


def to_shared_evidence_ref(evidence: EvidenceReference) -> EvidenceRef:
    """Convert the local compatibility model to the shared provenance contract."""
    return EvidenceRef(
        source_doc_id=evidence.source_document_id,
        page_number=evidence.page_number,
        section_hint=evidence.section_hint,
        snippet_anchor=evidence.snippet_anchor,
        excerpt=evidence.excerpt,
        method=evidence.method if evidence.method in ExtractionMethod.__args__ else None,
    )
