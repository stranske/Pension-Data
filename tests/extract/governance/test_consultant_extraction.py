"""Tests for consultant engagement and recommendation attribution extraction."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pension_data.extract.governance.consultants import (
    AttributionMention,
    ConsultantMention,
    RecommendationMention,
    extract_consultant_records,
    normalize_attribution_strength,
    normalize_board_decision_status,
)

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "consultant_extraction_golden.json"


def _load_fixture() -> dict[str, Any]:
    with FIXTURE_PATH.open(encoding="utf-8") as handle:
        return json.load(handle)


def _consultant_mentions(payload: list[dict[str, Any]]) -> list[ConsultantMention]:
    return [
        ConsultantMention(
            consultant_name=item.get("consultant_name"),
            role_description=item.get("role_description"),
            confidence=item.get("confidence", 1.0),
            evidence_refs=tuple(item.get("evidence_refs", [])),
            source_url=item.get("source_url", "not_disclosed"),
        )
        for item in payload
    ]


def _recommendation_mentions(payload: list[dict[str, Any]]) -> list[RecommendationMention]:
    return [
        RecommendationMention(
            consultant_name=item.get("consultant_name"),
            topic=item.get("topic"),
            recommendation_text=item.get("recommendation_text"),
            board_decision_status=item.get("board_decision_status"),
            confidence=item.get("confidence", 1.0),
            evidence_refs=tuple(item.get("evidence_refs", [])),
            source_url=item.get("source_url", "not_disclosed"),
        )
        for item in payload
    ]


def _attribution_mentions(payload: list[dict[str, Any]]) -> list[AttributionMention]:
    return [
        AttributionMention(
            consultant_name=item.get("consultant_name"),
            topic=item.get("topic"),
            observed_outcome=item.get("observed_outcome"),
            strength=item.get("strength"),
            confidence=item.get("confidence", 1.0),
            evidence_refs=tuple(item.get("evidence_refs", [])),
            source_url=item.get("source_url", "not_disclosed"),
        )
        for item in payload
    ]


def test_consultant_rich_fixture_extracts_engagement_recommendation_and_attribution() -> None:
    fixture = _load_fixture()["consultant_rich"]
    extracted = extract_consultant_records(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        consultant_mentions=_consultant_mentions(fixture["consultant_mentions"]),
        recommendation_mentions=_recommendation_mentions(fixture["recommendation_mentions"]),
        attribution_mentions=_attribution_mentions(fixture["attribution_mentions"]),
    )

    entities = extracted["consultant_entities"]
    engagements = extracted["plan_consultant_engagements"]
    recommendations = extracted["consultant_recommendations"]
    attributions = extracted["consultant_attribution_observations"]
    warnings = extracted["warnings"]

    assert len(entities) == 2
    assert len(engagements) == 2
    assert len(recommendations) == 1
    assert len(attributions) == 1
    assert warnings == []
    assert recommendations[0].board_decision_status == "adopted"
    assert attributions[0].strength == "explicit"
    assert recommendations[0].evidence_refs == ("p20",)
    assert attributions[0].source_metadata["source_url"] == "https://example.org/ca-pers-2025.pdf"


def test_consultant_sparse_fixture_persists_non_disclosure_rows() -> None:
    fixture = _load_fixture()["consultant_sparse"]
    extracted = extract_consultant_records(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        consultant_mentions=[],
        recommendation_mentions=[],
        attribution_mentions=[],
    )

    engagements = extracted["plan_consultant_engagements"]
    recommendations = extracted["consultant_recommendations"]
    attributions = extracted["consultant_attribution_observations"]
    warnings = extracted["warnings"]

    assert len(engagements) == 1
    assert engagements[0].consultant_name == "not_disclosed"
    assert not engagements[0].is_disclosed
    assert len(recommendations) == 1
    assert recommendations[0].board_decision_status == "not_disclosed"
    assert len(attributions) == 1
    assert attributions[0].strength == "speculative"
    assert any(warning.code == "non_disclosure" for warning in warnings)


def test_normalization_maps_statuses_and_strength_labels() -> None:
    assert normalize_board_decision_status("approved") == "adopted"
    assert normalize_board_decision_status("partially approved") == "partially_adopted"
    assert normalize_board_decision_status("declined") == "rejected"
    assert normalize_board_decision_status("unknown text") == "not_disclosed"

    assert normalize_attribution_strength("direct") == "explicit"
    assert normalize_attribution_strength("inferred") == "implied"
    assert normalize_attribution_strength("uncertain") == "speculative"
    assert normalize_attribution_strength("unexpected") == "speculative"


def test_extraction_output_is_reproducible() -> None:
    fixture = _load_fixture()["consultant_rich"]
    consultant_mentions = _consultant_mentions(fixture["consultant_mentions"])
    recommendation_mentions = _recommendation_mentions(fixture["recommendation_mentions"])
    attribution_mentions = _attribution_mentions(fixture["attribution_mentions"])

    first = extract_consultant_records(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        consultant_mentions=consultant_mentions,
        recommendation_mentions=recommendation_mentions,
        attribution_mentions=attribution_mentions,
    )
    second = extract_consultant_records(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        consultant_mentions=consultant_mentions,
        recommendation_mentions=recommendation_mentions,
        attribution_mentions=attribution_mentions,
    )

    assert first == second
