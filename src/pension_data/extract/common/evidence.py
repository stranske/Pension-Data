"""Evidence-reference parsing and canonicalization helpers."""

from __future__ import annotations

import hashlib
import json
import re

from pension_data.db.models.provenance import EvidenceReference

_PAGE_REF_PATTERN = re.compile(
    r"^(?:p(?:age)?[\s\.:#-]*)(?P<page>\d+)(?:\s*(?:#|:|-)\s*(?P<section>.+))?$",
    re.IGNORECASE,
)


def _stable_id(prefix: str, *parts: object) -> str:
    encoded_parts = [json.dumps(part, sort_keys=True) for part in parts]
    digest = hashlib.sha256("|".join(encoded_parts).encode("utf-8")).hexdigest()[:20]
    return f"{prefix}:{digest}"


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
    return normalized or "table:unknown"


def build_evidence_reference(
    *,
    report_id: str,
    source_document_id: str,
    evidence_ref: str,
) -> EvidenceReference:
    """Parse one evidence ref into structured page/anchor fields."""
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

    return EvidenceReference(
        evidence_ref_id=_stable_id(
            "evidence",
            report_id,
            source_document_id,
            normalized_ref,
            page_number,
            section_hint,
            snippet_anchor,
        ),
        report_id=report_id,
        source_document_id=source_document_id,
        raw_ref=normalized_ref,
        page_number=page_number,
        section_hint=section_hint,
        snippet_anchor=snippet_anchor,
    )
