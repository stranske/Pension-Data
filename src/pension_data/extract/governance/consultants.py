"""Consultant extraction patterns for engagement, recommendations, and attribution."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal

from pension_data.db.models.consultant_attribution import (
    AttributionStrength,
    ConsultantAttributionObservation,
)
from pension_data.db.models.consultants import (
    BoardDecisionStatus,
    ConsultantEntity,
    ConsultantRecommendation,
    PlanConsultantEngagement,
)

ConsultantWarningCode = Literal["non_disclosure", "ambiguous_naming", "missing_topic"]

_BOARD_STATUS_NORMALIZATION: dict[str, BoardDecisionStatus] = {
    "adopted": "adopted",
    "approved": "adopted",
    "partially adopted": "partially_adopted",
    "partially approved": "partially_adopted",
    "rejected": "rejected",
    "declined": "rejected",
    "not disclosed": "not_disclosed",
    "unknown": "not_disclosed",
}
_ATTRIBUTION_STRENGTH_NORMALIZATION: dict[str, AttributionStrength] = {
    "explicit": "explicit",
    "direct": "explicit",
    "implied": "implied",
    "inferred": "implied",
    "speculative": "speculative",
    "uncertain": "speculative",
}
_WARNING_MESSAGES: dict[ConsultantWarningCode, str] = {
    "non_disclosure": "Consultant content is not disclosed in this report section.",
    "ambiguous_naming": "Consultant naming is inconsistent across extracted rows.",
    "missing_topic": "Recommendation topic is missing and was marked as not_disclosed.",
}
_NON_DISCLOSED_NAMES = frozenset(
    {
        "n/a",
        "na",
        "none",
        "not disclosed",
        "not_disclosed",
        "undisclosed",
        "unknown",
    }
)


@dataclass(frozen=True, slots=True)
class ConsultantMention:
    """Raw consultant engagement mention extracted from governance text/tables."""

    consultant_name: str | None
    role_description: str | None
    confidence: float
    evidence_refs: tuple[str, ...]
    source_url: str


@dataclass(frozen=True, slots=True)
class RecommendationMention:
    """Raw recommendation mention with optional board decision context."""

    consultant_name: str | None
    topic: str | None
    recommendation_text: str | None
    board_decision_status: str | None
    confidence: float
    evidence_refs: tuple[str, ...]
    source_url: str


@dataclass(frozen=True, slots=True)
class AttributionMention:
    """Raw attribution mention linking recommendation to an outcome."""

    consultant_name: str | None
    topic: str | None
    observed_outcome: str | None
    strength: str | None
    confidence: float
    evidence_refs: tuple[str, ...]
    source_url: str


@dataclass(frozen=True, slots=True)
class ConsultantExtractionWarning:
    """Extraction warning for consultant governance coverage quality."""

    code: ConsultantWarningCode
    plan_id: str
    plan_period: str
    message: str
    evidence_refs: tuple[str, ...]


def _normalize_name(value: str | None) -> str:
    if value is None:
        return ""
    return " ".join(value.strip().lower().split())


def _clean_text(value: str | None, *, fallback: str) -> str:
    if value is None:
        return fallback
    normalized = value.strip()
    return normalized if normalized else fallback


def _bounded_confidence(value: float) -> float:
    return round(max(0.0, min(1.0, value)), 6)


def _dedupe_refs(evidence_refs: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(ref.strip() for ref in evidence_refs if ref.strip()))


def _stable_refs_from_mentions(mentions: Iterable[ConsultantMention]) -> tuple[str, ...]:
    return tuple(
        sorted(
            {ref.strip() for mention in mentions for ref in mention.evidence_refs if ref.strip()}
        )
    )


def _primary_source_url(source_urls: Iterable[str]) -> str:
    normalized = sorted({source_url.strip() for source_url in source_urls if source_url.strip()})
    if normalized:
        return normalized[0]
    return "not_disclosed"


def _source_metadata(source_url: str) -> MappingProxyType[str, str]:
    return MappingProxyType({"source_url": source_url, "source_type": "annual_report"})


_NOT_DISCLOSED_SOURCE_METADATA = _source_metadata("not_disclosed")


def _is_disclosed_name(value: str | None) -> bool:
    normalized_name = _normalize_name(value)
    return bool(normalized_name) and normalized_name not in _NON_DISCLOSED_NAMES


def normalize_board_decision_status(value: str | None) -> BoardDecisionStatus:
    """Normalize board decision status into controlled literals."""
    if value is None:
        return "not_disclosed"
    normalized = " ".join(value.strip().lower().split())
    return _BOARD_STATUS_NORMALIZATION.get(normalized, "not_disclosed")


def normalize_attribution_strength(value: str | None) -> AttributionStrength:
    """Normalize attribution strength into explicit/implied/speculative tiers."""
    if value is None:
        return "speculative"
    normalized = " ".join(value.strip().lower().split())
    return _ATTRIBUTION_STRENGTH_NORMALIZATION.get(normalized, "speculative")


def extract_consultant_records(
    *,
    plan_id: str,
    plan_period: str,
    consultant_mentions: list[ConsultantMention],
    recommendation_mentions: list[RecommendationMention],
    attribution_mentions: list[AttributionMention],
) -> dict[str, object]:
    """Extract consultant entities, engagements, recommendations, and attribution observations."""
    warnings: list[ConsultantExtractionWarning] = []

    entities: list[ConsultantEntity] = []
    name_groups: dict[str, list[ConsultantMention]] = {}
    for consultant_mention in consultant_mentions:
        normalized_name = _normalize_name(consultant_mention.consultant_name)
        if not normalized_name:
            continue
        name_groups.setdefault(normalized_name, []).append(consultant_mention)

    for normalized_name, consultant_group in sorted(name_groups.items()):
        display_names = {
            _clean_text(consultant_mention.consultant_name, fallback="not_disclosed")
            for consultant_mention in consultant_group
        }
        merged_refs = _stable_refs_from_mentions(consultant_group)
        if len(display_names) > 1:
            warnings.append(
                ConsultantExtractionWarning(
                    code="ambiguous_naming",
                    plan_id=plan_id,
                    plan_period=plan_period,
                    message=_WARNING_MESSAGES["ambiguous_naming"],
                    evidence_refs=merged_refs,
                )
            )

        entity = ConsultantEntity(
            consultant_name=sorted(display_names)[0],
            normalized_name=normalized_name,
            confidence=max(
                _bounded_confidence(consultant_mention.confidence)
                for consultant_mention in consultant_group
            ),
            evidence_refs=merged_refs,
            source_metadata=_source_metadata(
                _primary_source_url(
                    consultant_mention.source_url for consultant_mention in consultant_group
                )
            ),
        )
        entities.append(entity)

    engagements = [
        PlanConsultantEngagement(
            plan_id=plan_id,
            plan_period=plan_period,
            consultant_name=_clean_text(
                consultant_mention.consultant_name, fallback="not_disclosed"
            ),
            role_description=_clean_text(
                consultant_mention.role_description, fallback="not_disclosed"
            ),
            is_disclosed=_is_disclosed_name(consultant_mention.consultant_name),
            confidence=_bounded_confidence(consultant_mention.confidence),
            evidence_refs=_dedupe_refs(consultant_mention.evidence_refs),
            source_metadata=_source_metadata(consultant_mention.source_url),
        )
        for consultant_mention in sorted(
            consultant_mentions,
            key=lambda row: (
                _normalize_name(row.consultant_name),
                _clean_text(row.role_description, fallback=""),
            ),
        )
    ]

    if not engagements:
        engagements.append(
            PlanConsultantEngagement(
                plan_id=plan_id,
                plan_period=plan_period,
                consultant_name="not_disclosed",
                role_description="not_disclosed",
                is_disclosed=False,
                confidence=0.0,
                evidence_refs=(),
                source_metadata=_NOT_DISCLOSED_SOURCE_METADATA,
            )
        )
        warnings.append(
            ConsultantExtractionWarning(
                code="non_disclosure",
                plan_id=plan_id,
                plan_period=plan_period,
                message=_WARNING_MESSAGES["non_disclosure"],
                evidence_refs=(),
            )
        )

    recommendations: list[ConsultantRecommendation] = []
    for recommendation_mention in sorted(
        recommendation_mentions,
        key=lambda row: (
            _normalize_name(row.consultant_name),
            _clean_text(row.topic, fallback=""),
            _clean_text(row.recommendation_text, fallback=""),
        ),
    ):
        topic = _clean_text(recommendation_mention.topic, fallback="not_disclosed")
        if topic == "not_disclosed":
            warnings.append(
                ConsultantExtractionWarning(
                    code="missing_topic",
                    plan_id=plan_id,
                    plan_period=plan_period,
                    message=_WARNING_MESSAGES["missing_topic"],
                    evidence_refs=_dedupe_refs(recommendation_mention.evidence_refs),
                )
            )
        recommendations.append(
            ConsultantRecommendation(
                plan_id=plan_id,
                plan_period=plan_period,
                consultant_name=_clean_text(
                    recommendation_mention.consultant_name, fallback="not_disclosed"
                ),
                topic=topic,
                recommendation_text=_clean_text(
                    recommendation_mention.recommendation_text,
                    fallback="not_disclosed",
                ),
                board_decision_status=normalize_board_decision_status(
                    recommendation_mention.board_decision_status
                ),
                confidence=_bounded_confidence(recommendation_mention.confidence),
                evidence_refs=_dedupe_refs(recommendation_mention.evidence_refs),
                source_metadata=_source_metadata(recommendation_mention.source_url),
            )
        )

    if not recommendations:
        recommendations.append(
            ConsultantRecommendation(
                plan_id=plan_id,
                plan_period=plan_period,
                consultant_name="not_disclosed",
                topic="not_disclosed",
                recommendation_text="not_disclosed",
                board_decision_status="not_disclosed",
                confidence=0.0,
                evidence_refs=(),
                source_metadata=_NOT_DISCLOSED_SOURCE_METADATA,
            )
        )

    attributions: list[ConsultantAttributionObservation] = [
        ConsultantAttributionObservation(
            plan_id=plan_id,
            plan_period=plan_period,
            consultant_name=_clean_text(attr_mention.consultant_name, fallback="not_disclosed"),
            recommendation_topic=_clean_text(attr_mention.topic, fallback="not_disclosed"),
            observed_outcome=_clean_text(attr_mention.observed_outcome, fallback="not_disclosed"),
            strength=normalize_attribution_strength(attr_mention.strength),
            confidence=_bounded_confidence(attr_mention.confidence),
            evidence_refs=_dedupe_refs(attr_mention.evidence_refs),
            source_metadata=_source_metadata(attr_mention.source_url),
        )
        for attr_mention in sorted(
            attribution_mentions,
            key=lambda row: (
                _normalize_name(row.consultant_name),
                _clean_text(row.topic, fallback=""),
                _clean_text(row.observed_outcome, fallback=""),
            ),
        )
    ]
    if not attributions:
        attributions.append(
            ConsultantAttributionObservation(
                plan_id=plan_id,
                plan_period=plan_period,
                consultant_name="not_disclosed",
                recommendation_topic="not_disclosed",
                observed_outcome="not_disclosed",
                strength="speculative",
                confidence=0.0,
                evidence_refs=(),
                source_metadata=_NOT_DISCLOSED_SOURCE_METADATA,
            )
        )

    warnings = sorted(
        warnings,
        key=lambda warning: (
            warning.code,
            warning.plan_id,
            warning.plan_period,
            warning.evidence_refs,
        ),
    )
    return {
        "consultant_entities": entities,
        "plan_consultant_engagements": engagements,
        "consultant_recommendations": recommendations,
        "consultant_attribution_observations": attributions,
        "warnings": warnings,
    }
