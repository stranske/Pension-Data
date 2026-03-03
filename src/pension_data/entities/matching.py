"""Alias candidate matching and confidence scoring helpers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Literal

from pension_data.normalize.entity_tokens import normalize_entity_token

MatchStrategy = Literal["exact", "normalized", "fuzzy"]


@dataclass(frozen=True, slots=True)
class CanonicalEntityAliasRecord:
    """Canonical entity alias surface for candidate matching."""

    stable_id: str
    canonical_name: str
    aliases: tuple[str, ...] = ()

    @property
    def canonical_entity_id(self) -> str:
        """Compatibility alias clarifying that `stable_id` is the canonical entity ID."""
        return self.stable_id


@dataclass(frozen=True, slots=True)
class AliasMatchCandidate:
    """One candidate mapping from source alias text to canonical entity ID."""

    source_name: str
    stable_id: str
    strategy: MatchStrategy
    confidence: float

    @property
    def canonical_entity_id(self) -> str:
        """Compatibility alias clarifying that `stable_id` is the canonical entity ID."""
        return self.stable_id


def _token_set(value: str) -> set[str]:
    normalized = normalize_entity_token(value)
    if not normalized:
        return set()
    return set(normalized.split())


def _normalized_forms(record: CanonicalEntityAliasRecord) -> tuple[str, ...]:
    forms = [record.canonical_name, *record.aliases]
    normalized: list[str] = []
    for value in forms:
        token = normalize_entity_token(value)
        if not token:
            continue
        normalized.append(token)
    return tuple(dict.fromkeys(normalized))


def generate_alias_match_candidates(
    *,
    source_name: str,
    entities: Sequence[CanonicalEntityAliasRecord],
    min_fuzzy_confidence: float = 0.72,
) -> list[AliasMatchCandidate]:
    """Generate deterministic alias match candidates using exact/normalized/fuzzy strategies."""
    normalized_source = normalize_entity_token(source_name)
    if not normalized_source:
        return []

    source_tokens = _token_set(source_name)
    by_stable_id: dict[str, AliasMatchCandidate] = {}

    for record in entities:
        forms = _normalized_forms(record)
        if not forms:
            continue

        strategy: MatchStrategy | None = None
        confidence = 0.0

        if normalized_source in forms:
            strategy = "exact"
            confidence = 1.0
        else:
            overlap_scores: list[float] = []
            for form in forms:
                form_tokens = _token_set(form)
                if not source_tokens or not form_tokens:
                    continue
                overlap = len(source_tokens.intersection(form_tokens)) / len(
                    source_tokens.union(form_tokens)
                )
                overlap_scores.append(overlap)
            best_overlap = max(overlap_scores) if overlap_scores else 0.0
            if best_overlap >= 0.86:
                strategy = "normalized"
                confidence = round(best_overlap, 4)
            else:
                ratios = [SequenceMatcher(a=normalized_source, b=form).ratio() for form in forms]
                best_ratio = max(ratios) if ratios else 0.0
                if best_ratio >= min_fuzzy_confidence:
                    strategy = "fuzzy"
                    confidence = round(best_ratio, 4)

        if strategy is None:
            continue

        candidate = AliasMatchCandidate(
            source_name=source_name,
            stable_id=record.stable_id,
            strategy=strategy,
            confidence=confidence,
        )
        existing = by_stable_id.get(record.stable_id)
        if existing is None or candidate.confidence > existing.confidence:
            by_stable_id[record.stable_id] = candidate

    return sorted(
        by_stable_id.values(),
        key=lambda item: (-item.confidence, item.stable_id),
    )
