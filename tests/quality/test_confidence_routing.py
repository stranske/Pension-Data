"""Tests for confidence threshold routing policy."""

from __future__ import annotations

from pension_data.quality.confidence import (
    AUTO_ACCEPT_THRESHOLD,
    WARNING_QUEUE_THRESHOLD,
    ExtractionConfidenceInput,
    route_confidence_row,
    route_confidence_rows,
)


def test_threshold_boundaries_route_exactly_at_075_and_090() -> None:
    low_boundary = route_confidence_row(
        ExtractionConfidenceInput(
            row_id="row:075",
            plan_id="ca-pers",
            plan_period="FY2025",
            metric_name="funded_ratio",
            confidence=WARNING_QUEUE_THRESHOLD,
            evidence_refs=("p.40",),
        )
    )
    high_boundary = route_confidence_row(
        ExtractionConfidenceInput(
            row_id="row:090",
            plan_id="ca-pers",
            plan_period="FY2025",
            metric_name="funded_ratio",
            confidence=AUTO_ACCEPT_THRESHOLD,
            evidence_refs=("p.41",),
        )
    )

    assert low_boundary.routing_outcome == "publish_with_warning"
    assert low_boundary.review_priority == "medium"
    assert high_boundary.routing_outcome == "auto_accept"
    assert high_boundary.review_priority == "none"


def test_low_confidence_routes_to_high_priority_review_queue() -> None:
    decision = route_confidence_row(
        ExtractionConfidenceInput(
            row_id="row:low",
            plan_id="tx-ers",
            plan_period="FY2025",
            metric_name="allocation_public_equity",
            confidence=0.74,
            evidence_refs=("p.22",),
        )
    )

    assert decision.routing_outcome == "high_priority_review"
    assert decision.review_priority == "high"


def test_publication_is_not_blocked_by_routing_outcome() -> None:
    decisions = route_confidence_rows(
        [
            ExtractionConfidenceInput(
                row_id="row:auto",
                plan_id="wa-srs",
                plan_period="FY2025",
                metric_name="funded_ratio",
                confidence=0.95,
            ),
            ExtractionConfidenceInput(
                row_id="row:warn",
                plan_id="wa-srs",
                plan_period="FY2025",
                metric_name="discount_rate",
                confidence=0.80,
            ),
            ExtractionConfidenceInput(
                row_id="row:review",
                plan_id="wa-srs",
                plan_period="FY2025",
                metric_name="manager_fee_rate",
                confidence=0.55,
            ),
        ]
    )

    assert len(decisions) == 3
    assert all(decision.publish_blocked is False for decision in decisions)
    assert {decision.routing_outcome for decision in decisions} == {
        "auto_accept",
        "publish_with_warning",
        "high_priority_review",
    }
