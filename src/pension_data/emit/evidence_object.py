"""Project persisted metric evidence into standalone ``evidence-object/v1`` files."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from typing import Any

from pension_data.db.models.provenance import EvidenceMethod, EvidenceReference
from pension_data.extract.common.evidence import build_evidence_reference
from pension_data.normalize.numeric_parsing import (
    parse_numeric_token,
    truncate_at_sentence_boundary,
)

_METRIC_ALIASES: dict[str, tuple[str, ...]] = {
    "funded_ratio": ("funded ratio", "funding ratio"),
    "aal_usd": ("aal", "actuarial accrued liability"),
    "ava_usd": ("ava", "actuarial value of assets"),
    "discount_rate": ("discount rate", "assumed return"),
    "employer_contribution_rate": ("employer contribution rate", "adc rate"),
    "employee_contribution_rate": ("employee contribution rate",),
    "participant_count": ("participant count", "active participants"),
}


def _non_empty_string(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


def _evidence_method(extraction_method: object) -> EvidenceMethod:
    method = _non_empty_string(extraction_method, field="extraction_method")
    if method == "table_lookup":
        return "table"
    if method == "text_pattern":
        return "text"
    if method in {"fallback", "ocr", "llm"}:
        return method  # type: ignore[return-value]
    raise ValueError(f"unsupported extraction_method for evidence emission: {method}")


def _matches_metric(label_or_text: str, metric_name: str) -> bool:
    normalized = " ".join(label_or_text.lower().replace("_", " ").split())
    aliases = _METRIC_ALIASES.get(metric_name, (metric_name.replace("_", " "),))
    return any(alias in normalized for alias in aliases)


def _matching_metric_position(
    text: str, metric_name: str, as_reported_value: float | None
) -> int | None:
    """Locate the metric mention that supports the persisted selected value."""
    lowered = text.lower()
    aliases = _METRIC_ALIASES.get(metric_name, (metric_name.replace("_", " "),))
    fallback_position: int | None = None
    for alias in aliases:
        start_index = 0
        while True:
            match_index = lowered.find(alias, start_index)
            if match_index == -1:
                break
            fallback_position = match_index if fallback_position is None else fallback_position
            if as_reported_value is None:
                return match_index
            value_window = truncate_at_sentence_boundary(
                text[match_index + len(alias) : match_index + len(alias) + 96]
            )
            parsed = parse_numeric_token(value_window)
            if parsed is not None and math.isclose(
                parsed, as_reported_value, rel_tol=1e-9, abs_tol=1e-9
            ):
                return match_index
            start_index = match_index + len(alias)
    return fallback_position if as_reported_value is None else None


def _reported_value(row: Mapping[str, object]) -> float | None:
    value = row.get("as_reported_value")
    if isinstance(value, int | float) and not isinstance(value, bool) and math.isfinite(value):
        return float(value)
    return None


def _bounded_text_excerpt(text: str, *, metric_position: int) -> str:
    """Keep a long source block bounded around the supporting metric mention."""
    cleaned = text.strip()
    if len(cleaned) <= 2000:
        return cleaned
    start = max(0, metric_position - 500)
    end = min(len(cleaned), start + 2000)
    if end - start < 2000:
        start = max(0, end - 2000)
    return cleaned[start:end].strip()


def _grounded_excerpt(
    *,
    metric_name: str,
    evidence_ref: str,
    method: EvidenceMethod,
    source_evidence: Mapping[str, object],
    as_reported_value: float | None,
) -> str:
    if method == "table":
        rows = source_evidence.get("table_rows", [])
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict) or row.get("evidence_ref") != evidence_ref:
                    continue
                label = row.get("label")
                value = row.get("value")
                if (
                    isinstance(label, str)
                    and isinstance(value, str)
                    and value.strip()
                    and _matches_metric(label, metric_name)
                ):
                    parsed = parse_numeric_token(value)
                    if as_reported_value is not None and (
                        parsed is None
                        or not math.isclose(parsed, as_reported_value, rel_tol=1e-9, abs_tol=1e-9)
                    ):
                        continue
                    return value.strip()[:2000]
    else:
        blocks = source_evidence.get("text_blocks", [])
        if isinstance(blocks, list):
            for block in blocks:
                if not isinstance(block, dict) or block.get("evidence_ref") != evidence_ref:
                    continue
                excerpt = block.get("excerpt")
                if (
                    isinstance(excerpt, str)
                    and excerpt.strip()
                    and _matches_metric(excerpt, metric_name)
                ):
                    position = _matching_metric_position(excerpt, metric_name, as_reported_value)
                    if position is None:
                        continue
                    return _bounded_text_excerpt(excerpt, metric_position=position)
    raise ValueError(
        f"no grounded excerpt for metric '{metric_name}' at evidence ref '{evidence_ref}'"
    )


def _locator_payload(reference: EvidenceReference) -> dict[str, object]:
    locator: dict[str, object] = {}
    page_number = reference.page_number
    section_hint = reference.section_hint
    snippet_anchor = reference.snippet_anchor
    if page_number is not None:
        locator["page"] = page_number
    if section_hint is not None:
        locator["section"] = section_hint
    if snippet_anchor is not None:
        locator["snippet_anchor"] = snippet_anchor
    return locator


def _content_id(payload: Mapping[str, object]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return f"evidence:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


def build_evidence_objects(
    *,
    run_id: str,
    core_rows: Sequence[Mapping[str, object]],
    source_evidence: Mapping[str, object],
    pension_entity_ref: str,
) -> list[dict[str, Any]]:
    """Build deterministic, grounded evidence objects for persisted core facts."""
    emitted: dict[str, dict[str, Any]] = {}
    for row in core_rows:
        fact_ref = _non_empty_string(row.get("fact_id"), field="fact_id")
        metric_name = _non_empty_string(row.get("metric_name"), field="metric_name")
        source_id = _non_empty_string(row.get("source_document_id"), field="source_document_id")
        method = _evidence_method(row.get("extraction_method"))
        refs = row.get("evidence_refs")
        if not isinstance(refs, list) or not refs:
            raise ValueError(f"fact '{fact_ref}' has no evidence_refs")
        for raw_ref in refs:
            evidence_ref = _non_empty_string(raw_ref, field="evidence_ref")
            excerpt = _grounded_excerpt(
                metric_name=metric_name,
                evidence_ref=evidence_ref,
                method=method,
                source_evidence=source_evidence,
                as_reported_value=_reported_value(row),
            )
            parsed = build_evidence_reference(
                report_id=run_id,
                source_document_id=source_id,
                evidence_ref=evidence_ref,
                excerpt=excerpt,
                method=method,
            )
            payload: dict[str, Any] = {
                "schema_version": "evidence-object/v1",
                "fact_ref": fact_ref,
                "source_id": source_id,
                "method": method,
                "excerpt": excerpt,
                "locator": _locator_payload(parsed),
                "entity_ref": pension_entity_ref,
            }
            confidence = row.get("confidence")
            if isinstance(confidence, int | float) and not isinstance(confidence, bool):
                payload["confidence"] = confidence
            payload["evidence_id"] = _content_id(payload)
            emitted[payload["evidence_id"]] = payload
    return [emitted[evidence_id] for evidence_id in sorted(emitted)]
