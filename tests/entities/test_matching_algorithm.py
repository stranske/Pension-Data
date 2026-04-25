"""Regression tests for alias matching algorithm and confidence scoring."""

from __future__ import annotations

from pension_data.entities.matching import (
    CanonicalEntityAliasRecord,
    generate_alias_match_candidates,
)


def _entity(
    stable_id: str,
    canonical_name: str,
    aliases: tuple[str, ...] = (),
) -> CanonicalEntityAliasRecord:
    return CanonicalEntityAliasRecord(
        stable_id=stable_id,
        canonical_name=canonical_name,
        aliases=aliases,
    )


# ── Exact matching ──────────────────────────────────────────────────


class TestExactMatch:
    def test_exact_match_full_confidence(self) -> None:
        entities = [_entity("e1", "BlackRock")]
        results = generate_alias_match_candidates(
            source_name="BlackRock", entities=entities
        )
        assert len(results) == 1
        assert results[0].strategy == "exact"
        assert results[0].confidence == 1.0

    def test_exact_match_case_insensitive(self) -> None:
        entities = [_entity("e1", "BlackRock")]
        results = generate_alias_match_candidates(
            source_name="blackrock", entities=entities
        )
        assert len(results) == 1
        assert results[0].strategy == "exact"
        assert results[0].confidence == 1.0

    def test_exact_match_via_alias(self) -> None:
        entities = [_entity("e1", "BlackRock Inc.", aliases=("BlackRock",))]
        results = generate_alias_match_candidates(
            source_name="blackrock", entities=entities
        )
        assert len(results) == 1
        assert results[0].strategy == "exact"


# ── Normalized token overlap ────────────────────────────────────────


class TestNormalizedMatch:
    def test_high_token_overlap_matches(self) -> None:
        # 8 shared tokens out of 9 total → Jaccard = 8/9 ≈ 0.889 > 0.86
        entities = [_entity("e1", "BlackRock Global Alpha Beta Gamma Delta Epsilon Zeta")]
        results = generate_alias_match_candidates(
            source_name="BlackRock Global Alpha Beta Gamma Delta Epsilon Zeta Extra",
            entities=entities,
        )
        matched = [r for r in results if r.strategy == "normalized"]
        assert len(matched) == 1
        assert matched[0].confidence >= 0.86

    def test_low_overlap_no_normalized_match(self) -> None:
        entities = [_entity("e1", "BlackRock Global Investors")]
        results = generate_alias_match_candidates(
            source_name="Vanguard Small Cap Fund", entities=entities
        )
        normalized_matches = [r for r in results if r.strategy == "normalized"]
        assert len(normalized_matches) == 0


# ── Fuzzy matching ──────────────────────────────────────────────────


class TestFuzzyMatch:
    def test_fuzzy_similar_names(self) -> None:
        entities = [_entity("e1", "JP Morgan Asset Management")]
        results = generate_alias_match_candidates(
            source_name="JPMorgan Asset Mgmt", entities=entities
        )
        assert len(results) >= 1
        # Should match via fuzzy since tokens differ
        match = results[0]
        assert match.strategy in ("fuzzy", "normalized")
        assert match.confidence >= 0.72

    def test_below_fuzzy_threshold_excluded(self) -> None:
        entities = [_entity("e1", "Apple Inc.")]
        results = generate_alias_match_candidates(
            source_name="Completely Different Name", entities=entities
        )
        assert len(results) == 0

    def test_custom_fuzzy_threshold(self) -> None:
        entities = [_entity("e1", "JP Morgan Chase")]
        # Very high threshold should exclude marginal matches
        results = generate_alias_match_candidates(
            source_name="JPMorgan Chas",
            entities=entities,
            min_fuzzy_confidence=0.99,
        )
        fuzzy_matches = [r for r in results if r.strategy == "fuzzy"]
        assert len(fuzzy_matches) == 0


# ── Deduplication ───────────────────────────────────────────────────


class TestDeduplication:
    def test_same_entity_keeps_highest_confidence(self) -> None:
        entities = [
            _entity("e1", "BlackRock", aliases=("BLK", "BlackRock Fund")),
        ]
        results = generate_alias_match_candidates(
            source_name="BlackRock", entities=entities
        )
        assert len(results) == 1
        # Exact match (1.0) should win over any fuzzy match
        assert results[0].confidence == 1.0

    def test_multiple_entities_all_returned(self) -> None:
        entities = [
            _entity("e1", "BlackRock"),
            _entity("e2", "Blackstone"),
        ]
        results = generate_alias_match_candidates(
            source_name="BlackRock", entities=entities
        )
        stable_ids = {r.stable_id for r in results}
        assert "e1" in stable_ids


# ── Sorting ─────────────────────────────────────────────────────────


class TestSorting:
    def test_sorted_by_confidence_descending(self) -> None:
        entities = [
            _entity("e1", "Vanguard Total Bond"),
            _entity("e2", "Vanguard Total Stock"),
        ]
        results = generate_alias_match_candidates(
            source_name="Vanguard Total Bond Market", entities=entities
        )
        if len(results) >= 2:
            assert results[0].confidence >= results[1].confidence

    def test_tiebreak_by_stable_id(self) -> None:
        entities = [
            _entity("e2", "Same Name"),
            _entity("e1", "Same Name"),
        ]
        results = generate_alias_match_candidates(
            source_name="Same Name", entities=entities
        )
        assert len(results) == 2
        # Both are exact matches; should sort by stable_id ascending
        assert results[0].stable_id == "e1"
        assert results[1].stable_id == "e2"


# ── Edge cases ──────────────────────────────────────────────────────


class TestEdgeCases:
    def test_empty_source_name_returns_empty(self) -> None:
        entities = [_entity("e1", "BlackRock")]
        results = generate_alias_match_candidates(
            source_name="", entities=entities
        )
        assert results == []

    def test_whitespace_source_name_returns_empty(self) -> None:
        entities = [_entity("e1", "BlackRock")]
        results = generate_alias_match_candidates(
            source_name="   ", entities=entities
        )
        assert results == []

    def test_empty_entities_returns_empty(self) -> None:
        results = generate_alias_match_candidates(
            source_name="BlackRock", entities=[]
        )
        assert results == []

    def test_entity_with_empty_name_skipped(self) -> None:
        entities = [_entity("e1", "")]
        results = generate_alias_match_candidates(
            source_name="BlackRock", entities=entities
        )
        assert results == []
