"""Tests for alias ingestion, candidate matching, and routing decisions."""

from __future__ import annotations

from pension_data.entities.alias_pipeline import (
    build_alias_review_queue_candidates,
    capture_alias_observations,
    route_alias_observations,
)
from pension_data.entities.matching import (
    CanonicalEntityAliasRecord,
    generate_alias_match_candidates,
)


def _entities() -> list[CanonicalEntityAliasRecord]:
    return [
        CanonicalEntityAliasRecord(
            stable_id="manager:alpha capital",
            canonical_name="Alpha Capital",
            aliases=("Alpha Cap",),
        ),
        CanonicalEntityAliasRecord(
            stable_id="manager:beta partners",
            canonical_name="Beta Partners",
            aliases=("Beta Partner LP",),
        ),
        CanonicalEntityAliasRecord(
            stable_id="manager:acme advisors",
            canonical_name="Acme Advisors",
            aliases=(),
        ),
        CanonicalEntityAliasRecord(
            stable_id="manager:acme asset management",
            canonical_name="Acme Asset Management",
            aliases=("Acme Advisors",),
        ),
    ]


def test_capture_alias_observations_dedupes_and_preserves_provenance() -> None:
    captured = capture_alias_observations(
        source_record_id="row:1",
        source_field="manager_name",
        names=[" Alpha Capital ", "ALPHA CAPITAL", "", "Beta Partners"],
        evidence_refs=("p.12", "p.12", "p.13"),
    )

    assert [item.source_name for item in captured] == ["Alpha Capital", "Beta Partners"]
    assert all(item.source_record_id == "row:1" for item in captured)
    assert all(item.source_field == "manager_name" for item in captured)
    assert all(item.evidence_refs == ("p.12", "p.13") for item in captured)


def test_candidate_generation_covers_exact_normalized_and_fuzzy_controls() -> None:
    exact = generate_alias_match_candidates(
        source_name="Alpha Capital",
        entities=_entities(),
    )
    fuzzy = generate_alias_match_candidates(
        source_name="Alfa Capitel",
        entities=_entities(),
    )
    no_match = generate_alias_match_candidates(
        source_name="Unrelated Foundation Trust",
        entities=_entities(),
    )

    assert exact[0].stable_id == "manager:alpha capital"
    assert exact[0].strategy == "exact"
    assert fuzzy[0].stable_id == "manager:alpha capital"
    assert fuzzy[0].strategy in {"normalized", "fuzzy"}
    assert no_match == []


def test_routing_auto_links_high_confidence_and_reviews_ambiguous_candidates() -> None:
    observations = capture_alias_observations(
        source_record_id="row:2",
        source_field="manager_name",
        names=["Alpha Capital", "Acme Advisors"],
        evidence_refs=("p.40",),
    )
    decisions = route_alias_observations(observations, entities=_entities())

    auto_link = [item for item in decisions if item.source_name == "Alpha Capital"][0]
    ambiguous = [item for item in decisions if item.source_name == "Acme Advisors"][0]

    assert auto_link.status == "auto_link"
    assert auto_link.chosen_stable_id == "manager:alpha capital"
    assert auto_link.review_priority is None
    assert ambiguous.status == "review"
    assert ambiguous.chosen_stable_id is None
    assert ambiguous.review_priority == "medium"
    assert ambiguous.reason == "ambiguous top candidates"


def test_routing_sends_low_confidence_or_unmatched_aliases_to_review_queue() -> None:
    observations = capture_alias_observations(
        source_record_id="row:3",
        source_field="manager_name",
        names=["Betta Partnerz", "No Candidate Name"],
        evidence_refs=("p.55",),
    )
    decisions = route_alias_observations(observations, entities=_entities())
    queued = build_alias_review_queue_candidates(decisions)

    assert len(queued) == 2
    assert {item.source_name for item in queued} == {"Betta Partnerz", "No Candidate Name"}
    no_candidate = [item for item in queued if item.source_name == "No Candidate Name"][0]
    assert no_candidate.review_priority == "high"
    assert no_candidate.candidate_entity_ids == ()
    assert no_candidate.reason == "no viable candidates"
