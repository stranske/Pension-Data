"""Schema models and constants for source-quality aware ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

SourceAuthorityTier = Literal["official", "official-mirror", "high-confidence-third-party"]
MismatchReason = Literal["wrong_plan", "stale_period", "non_official_only"]
OfficialResolutionState = Literal[
    "available_official",
    "available_non_official_only",
    "not_found",
]
PaginationMode = Literal["single_page", "next_link", "query_param", "path_segment"]
DocumentFormatHint = Literal["html", "pdf", "mixed"]

SOURCE_AUTHORITY_TIERS: tuple[SourceAuthorityTier, ...] = (
    "official",
    "official-mirror",
    "high-confidence-third-party",
)
MISMATCH_REASONS: tuple[MismatchReason, ...] = (
    "wrong_plan",
    "stale_period",
    "non_official_only",
)
OFFICIAL_RESOLUTION_STATES: tuple[OfficialResolutionState, ...] = (
    "available_official",
    "available_non_official_only",
    "not_found",
)
PAGINATION_MODES: tuple[PaginationMode, ...] = (
    "single_page",
    "next_link",
    "query_param",
    "path_segment",
)
DOCUMENT_FORMAT_HINTS: tuple[DocumentFormatHint, ...] = ("html", "pdf", "mixed")

SYSTEM_OVERRIDE_KEYS: tuple[str, ...] = (
    "requires_javascript",
    "pagination_mode",
    "document_format_hint",
    "request_timeout_seconds",
    "rate_limit_per_minute",
    "force_canonical_host",
)


@dataclass(frozen=True, slots=True)
class SourceMapRecord:
    """A normalized source-map row with quality and identity controls."""

    plan_id: str
    plan_period: str
    cohort: str
    source_url: str
    source_authority_tier: SourceAuthorityTier
    official_resolution_state: OfficialResolutionState
    expected_plan_identity: str
    observed_plan_identity: str | None = None
    mismatch_reason: MismatchReason | None = None
    system_overrides: dict[str, bool | int | str] | None = None
