"""Shared contracts and sanitization helpers for findings LLM chains."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

_SECRET_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"sk-[A-Za-z0-9_-]{8,}"),
    re.compile(r"ghp_[A-Za-z0-9]{12,}"),
    re.compile(r"api[_-]?key\s*[:=]\s*[A-Za-z0-9._-]{8,}", re.IGNORECASE),
)


@dataclass(frozen=True, slots=True)
class FindingSlice:
    """One deterministic findings slice for explain/compare workflows."""

    slice_id: str
    plan_id: str
    plan_period: str
    metrics: Mapping[str, float]
    citations: tuple[str, ...]


def redact_sensitive_text(text: str) -> str:
    """Redact obvious secret-like material from output text."""
    cleaned = text
    for pattern in _SECRET_PATTERNS:
        cleaned = pattern.sub("[REDACTED]", cleaned)
    return cleaned


def normalize_text(value: object) -> str:
    """Normalize and redact one string-like value."""
    if not isinstance(value, str):
        return ""
    normalized = " ".join(value.strip().split())
    return redact_sensitive_text(normalized)


def normalize_string_tuple(values: object) -> tuple[str, ...]:
    """Normalize list-like strings with stable ordering and dedupe."""
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
        return ()
    normalized: list[str] = []
    for item in values:
        value = normalize_text(item)
        if value and value not in normalized:
            normalized.append(value)
    return tuple(normalized)


def filter_allowed_citations(
    *,
    proposed: Sequence[str],
    allowed: Sequence[str],
) -> tuple[str, ...]:
    """Filter proposed citations against an allowlist preserving user-facing order."""
    allowed_set = {item for item in allowed if item}
    selected: list[str] = []
    for citation in proposed:
        token = citation.strip()
        if token in allowed_set and token not in selected:
            selected.append(token)
    if selected:
        return tuple(selected)
    fallback: list[str] = []
    for citation in allowed:
        token = citation.strip()
        if token and token not in fallback:
            fallback.append(token)
    return tuple(fallback)


def ensure_question(question: str) -> str:
    """Validate a question payload for explain/compare requests."""
    normalized = normalize_text(question)
    if not normalized:
        raise ValueError("question is required")
    if len(normalized.split()) < 3:
        raise ValueError("question is ambiguous; provide a specific request")
    return normalized
