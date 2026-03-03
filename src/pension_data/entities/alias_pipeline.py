"""Alias capture and confidence-based routing for canonical entity linking."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from pension_data.entities.matching import (
    AliasMatchCandidate,
    CanonicalEntityAliasRecord,
    generate_alias_match_candidates,
)
from pension_data.extract.common.evidence import canonicalize_evidence_ref
from pension_data.normalize.entity_tokens import normalize_entity_token

AliasRoutingStatus = Literal["auto_link", "review"]
ReviewPriority = Literal["none", "high", "medium"]
QueueReviewPriority = Literal["high", "medium"]


@dataclass(frozen=True, slots=True)
class CapturedAliasObservation:
    """Captured source alias observation from extraction outputs/text fields."""

    source_name: str
    source_record_id: str
    source_field: str
    evidence_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AliasRoutingDecision:
    """Routing decision for one source alias observation."""

    source_name: str
    source_record_id: str
    source_field: str
    status: AliasRoutingStatus
    chosen_stable_id: str | None
    confidence: float
    review_priority: ReviewPriority
    reason: str
    candidates: tuple[AliasMatchCandidate, ...]
    evidence_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AliasReviewQueueCandidate:
    """Queue payload for unresolved/ambiguous alias candidates."""

    source_name: str
    source_record_id: str
    source_field: str
    candidate_entity_ids: tuple[str, ...]
    confidence: float
    review_priority: QueueReviewPriority
    reason: str
    evidence_refs: tuple[str, ...]


def _normalize_evidence_refs(values: Sequence[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for raw_ref in values:
        token = canonicalize_evidence_ref(raw_ref)
        if not token:
            continue
        normalized.append(token)
    return tuple(dict.fromkeys(normalized))


def _collapse_whitespace(value: str) -> str:
    return " ".join(value.split()).strip()


def _preferred_source_name(existing: str | None, candidate: str) -> str:
    if existing is None:
        return candidate
    if existing.isupper() != candidate.isupper():
        return candidate if existing.isupper() else existing
    if existing.casefold() != candidate.casefold():
        return candidate if candidate.casefold() < existing.casefold() else existing
    return candidate if candidate < existing else existing


def capture_alias_observations(
    *,
    source_record_id: str,
    source_field: str,
    names: Sequence[str],
    evidence_refs: Sequence[str] = (),
) -> list[CapturedAliasObservation]:
    """Capture deterministic alias observations from raw source names."""
    normalized_refs = _normalize_evidence_refs(evidence_refs)
    normalized_record_id = source_record_id.strip()
    normalized_source_field = source_field.strip()
    selected_name_by_normalized: dict[str, str] = {}
    for raw_name in names:
        collapsed_name = _collapse_whitespace(raw_name)
        normalized_name = normalize_entity_token(collapsed_name)
        if not normalized_name:
            continue
        selected_name_by_normalized[normalized_name] = _preferred_source_name(
            selected_name_by_normalized.get(normalized_name),
            collapsed_name,
        )

    observations: list[CapturedAliasObservation] = []
    for normalized_name in sorted(selected_name_by_normalized):
        observations.append(
            CapturedAliasObservation(
                source_name=selected_name_by_normalized[normalized_name],
                source_record_id=normalized_record_id,
                source_field=normalized_source_field,
                evidence_refs=normalized_refs,
            )
        )
    return observations


def route_alias_observations(
    observations: Sequence[CapturedAliasObservation],
    *,
    entities: Sequence[CanonicalEntityAliasRecord],
    auto_link_threshold: float = 0.92,
    review_threshold: float = 0.78,
    ambiguity_margin: float = 0.08,
) -> list[AliasRoutingDecision]:
    """Route aliases to auto-link or review queue using confidence and ambiguity controls."""
    decisions: list[AliasRoutingDecision] = []
    for observation in observations:
        candidates = generate_alias_match_candidates(
            source_name=observation.source_name,
            entities=entities,
        )
        if not candidates:
            decisions.append(
                AliasRoutingDecision(
                    source_name=observation.source_name,
                    source_record_id=observation.source_record_id,
                    source_field=observation.source_field,
                    status="review",
                    chosen_stable_id=None,
                    confidence=0.0,
                    review_priority="high",
                    reason="no viable candidates",
                    candidates=(),
                    evidence_refs=observation.evidence_refs,
                )
            )
            continue

        top = candidates[0]
        second_confidence = candidates[1].confidence if len(candidates) > 1 else 0.0
        confidence_gap = round(top.confidence - second_confidence, 4)
        is_ambiguous = len(candidates) > 1 and confidence_gap < ambiguity_margin
        should_auto_link = (
            top.confidence >= auto_link_threshold
            and top.strategy in {"exact", "normalized"}
            and not is_ambiguous
        )

        if should_auto_link:
            decisions.append(
                AliasRoutingDecision(
                    source_name=observation.source_name,
                    source_record_id=observation.source_record_id,
                    source_field=observation.source_field,
                    status="auto_link",
                    chosen_stable_id=top.stable_id,
                    confidence=top.confidence,
                    review_priority="none",
                    reason=f"high-confidence {top.strategy} match",
                    candidates=tuple(candidates),
                    evidence_refs=observation.evidence_refs,
                )
            )
            continue

        priority: ReviewPriority = "medium" if top.confidence >= review_threshold else "high"
        reason = (
            "ambiguous top candidates" if is_ambiguous else "confidence below auto-link threshold"
        )
        decisions.append(
            AliasRoutingDecision(
                source_name=observation.source_name,
                source_record_id=observation.source_record_id,
                source_field=observation.source_field,
                status="review",
                chosen_stable_id=None,
                confidence=top.confidence,
                review_priority=priority,
                reason=reason,
                candidates=tuple(candidates),
                evidence_refs=observation.evidence_refs,
            )
        )

    return sorted(
        decisions,
        key=lambda item: (
            item.status != "review",
            normalize_entity_token(item.source_name),
            item.source_record_id,
        ),
    )


def build_alias_review_queue_candidates(
    decisions: Sequence[AliasRoutingDecision],
) -> list[AliasReviewQueueCandidate]:
    """Build unresolved alias review-queue payloads from routing decisions."""
    queue_rows: list[AliasReviewQueueCandidate] = []
    for decision in decisions:
        if decision.status != "review" or decision.review_priority == "none":
            continue
        if decision.review_priority not in {"high", "medium"}:
            raise ValueError(
                "review decision must use high/medium priority, got "
                f"{decision.review_priority!r}"
            )
        queue_rows.append(
            AliasReviewQueueCandidate(
                source_name=decision.source_name,
                source_record_id=decision.source_record_id,
                source_field=decision.source_field,
                candidate_entity_ids=tuple(
                    item.canonical_entity_id for item in decision.candidates
                ),
                confidence=decision.confidence,
                review_priority=decision.review_priority,
                reason=decision.reason,
                evidence_refs=decision.evidence_refs,
            )
        )
    return sorted(
        queue_rows,
        key=lambda item: (
            item.review_priority != "high",
            normalize_entity_token(item.source_name),
            item.source_record_id,
        ),
    )
