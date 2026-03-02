"""Tests for anomaly rules and review-queue routing."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pension_data.quality.anomaly_rules import AnomalyThresholds, TimeSeriesPoint, detect_anomalies
from pension_data.review_queue.anomalies import route_anomalies_to_review_queue


def _point(
    *,
    plan_id: str,
    period: str,
    observed_at: datetime,
    funded_ratio: float,
    equity: float,
    fixed_income: float,
    confidence: float,
) -> TimeSeriesPoint:
    return TimeSeriesPoint(
        plan_id=plan_id,
        period=period,
        observed_at=observed_at,
        funded_ratio=funded_ratio,
        allocations={
            "public_equity": equity,
            "fixed_income": fixed_income,
        },
        confidence=confidence,
        evidence_refs=(f"doc:{plan_id}:{period}",),
        provenance={"source_url": f"https://example.org/{plan_id}/{period}"},
    )


def test_material_funded_shift_is_flagged_with_evidence_context() -> None:
    points = [
        _point(
            plan_id="ca-pers",
            period="2024",
            observed_at=datetime(2025, 1, 1, tzinfo=UTC),
            funded_ratio=0.82,
            equity=0.45,
            fixed_income=0.35,
            confidence=0.90,
        ),
        _point(
            plan_id="ca-pers",
            period="2025",
            observed_at=datetime(2026, 1, 1, tzinfo=UTC),
            funded_ratio=0.69,
            equity=0.40,
            fixed_income=0.40,
            confidence=0.92,
        ),
    ]

    anomalies = detect_anomalies(points)
    funded = [item for item in anomalies if item.metric == "funded_ratio"]

    assert len(funded) == 1
    assert funded[0].severity == "critical"
    assert funded[0].priority == "high"
    assert funded[0].score > 1.0
    assert funded[0].evidence_context["previous_period"] == "2024"
    assert funded[0].evidence_context["current_period"] == "2025"
    assert funded[0].evidence_context["metric_evidence"] == {
        "metric": "funded_ratio",
        "previous_value": 0.82,
        "current_value": 0.69,
        "signed_delta": -0.13,
        "absolute_delta": 0.13,
        "thresholds": {"warning": 0.05, "critical": 0.1},
    }


def test_small_shifts_do_not_trigger_false_positive_anomalies() -> None:
    points = [
        _point(
            plan_id="ny-retire",
            period="2024",
            observed_at=datetime(2025, 1, 1, tzinfo=UTC),
            funded_ratio=0.80,
            equity=0.50,
            fixed_income=0.30,
            confidence=0.90,
        ),
        _point(
            plan_id="ny-retire",
            period="2025",
            observed_at=datetime(2026, 1, 1, tzinfo=UTC),
            funded_ratio=0.82,
            equity=0.53,
            fixed_income=0.29,
            confidence=0.90,
        ),
    ]

    anomalies = detect_anomalies(points)
    assert anomalies == []


def test_low_confidence_anomaly_is_annotated_with_lower_priority() -> None:
    points = [
        _point(
            plan_id="tx-ers",
            period="2024",
            observed_at=datetime(2025, 1, 1, tzinfo=UTC),
            funded_ratio=0.79,
            equity=0.41,
            fixed_income=0.44,
            confidence=0.30,
        ),
        _point(
            plan_id="tx-ers",
            period="2025",
            observed_at=datetime(2026, 1, 1, tzinfo=UTC),
            funded_ratio=0.66,
            equity=0.56,
            fixed_income=0.28,
            confidence=0.28,
        ),
    ]

    anomalies = detect_anomalies(points)
    funded = [item for item in anomalies if item.metric == "funded_ratio"]

    assert len(funded) == 1
    assert funded[0].severity == "critical"
    assert funded[0].priority == "medium"
    assert funded[0].score > 0.0


def test_allocation_shift_thresholds_and_review_queue_routing() -> None:
    points = [
        _point(
            plan_id="wa-plan",
            period="2024",
            observed_at=datetime(2025, 1, 1, tzinfo=UTC),
            funded_ratio=0.87,
            equity=0.42,
            fixed_income=0.45,
            confidence=0.88,
        ),
        _point(
            plan_id="wa-plan",
            period="2025",
            observed_at=datetime(2026, 1, 1, tzinfo=UTC),
            funded_ratio=0.85,
            equity=0.57,
            fixed_income=0.30,
            confidence=0.87,
        ),
    ]

    anomalies = detect_anomalies(
        points, thresholds=AnomalyThresholds(allocation_shift_warning=0.08)
    )
    allocation = [item for item in anomalies if item.metric.startswith("allocation:")]

    assert allocation
    queue_items = route_anomalies_to_review_queue(
        anomalies,
        queued_at=datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
    )
    assert queue_items
    assert queue_items[0].priority in {"high", "medium"}
    assert "shift" in queue_items[0].reason
    assert queue_items[0].created_at == datetime(2026, 1, 2, 0, 0, tzinfo=UTC)
    metric_evidence = queue_items[0].evidence_context["metric_evidence"]
    assert isinstance(metric_evidence, dict)
    metric = metric_evidence["metric"]
    assert isinstance(metric, str)
    assert metric.startswith("allocation:")


def test_higher_shift_gets_higher_score_and_sorts_first_within_period() -> None:
    points = [
        _point(
            plan_id="or-pension",
            period="2024",
            observed_at=datetime(2025, 1, 1, tzinfo=UTC),
            funded_ratio=0.88,
            equity=0.40,
            fixed_income=0.50,
            confidence=0.90,
        ),
        _point(
            plan_id="or-pension",
            period="2025",
            observed_at=datetime(2026, 1, 1, tzinfo=UTC),
            funded_ratio=0.72,
            equity=0.58,
            fixed_income=0.32,
            confidence=0.90,
        ),
    ]

    anomalies = detect_anomalies(points)
    assert anomalies
    assert anomalies[0].score >= anomalies[1].score
    assert anomalies[0].period == anomalies[1].period == "2025"


def test_anomaly_thresholds_reject_out_of_range_values() -> None:
    with pytest.raises(ValueError, match=r"funded_shift_warning must be within \[0.0, 1.0\]"):
        AnomalyThresholds(funded_shift_warning=-0.01)

    with pytest.raises(
        ValueError,
        match=r"min_confidence_for_medium_priority must be within \[0.0, 1.0\]",
    ):
        AnomalyThresholds(min_confidence_for_medium_priority=1.01)


def test_anomaly_thresholds_require_warning_not_exceed_critical() -> None:
    with pytest.raises(
        ValueError,
        match="funded_shift_warning must be <= funded_shift_critical",
    ):
        AnomalyThresholds(funded_shift_warning=0.11, funded_shift_critical=0.10)

    with pytest.raises(
        ValueError,
        match="allocation_shift_warning must be <= allocation_shift_critical",
    ):
        AnomalyThresholds(allocation_shift_warning=0.13, allocation_shift_critical=0.12)


def test_detect_anomalies_orders_by_period_before_observed_at() -> None:
    points = [
        _point(
            plan_id="period-order",
            period="2024",
            observed_at=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            funded_ratio=0.80,
            equity=0.50,
            fixed_income=0.30,
            confidence=0.95,
        ),
        _point(
            plan_id="period-order",
            period="2025",
            observed_at=datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
            funded_ratio=0.60,
            equity=0.65,
            fixed_income=0.20,
            confidence=0.95,
        ),
    ]

    anomalies = detect_anomalies(points)
    assert anomalies
    funded = [item for item in anomalies if item.metric == "funded_ratio"][0]
    assert funded.period == "2025"
    assert funded.evidence_context["previous_period"] == "2024"
    assert funded.evidence_context["current_period"] == "2025"


def test_naive_datetimes_are_normalized_to_utc_in_evidence_and_queue() -> None:
    points = [
        _point(
            plan_id="naive-time",
            period="2024",
            observed_at=datetime(2025, 1, 1, 0, 0),
            funded_ratio=0.80,
            equity=0.50,
            fixed_income=0.30,
            confidence=0.95,
        ),
        _point(
            plan_id="naive-time",
            period="2025",
            observed_at=datetime(2026, 1, 1, 0, 0),
            funded_ratio=0.60,
            equity=0.65,
            fixed_income=0.20,
            confidence=0.95,
        ),
    ]
    anomalies = detect_anomalies(points)
    assert anomalies
    funded = [item for item in anomalies if item.metric == "funded_ratio"][0]
    assert funded.evidence_context["previous_observed_at"] == "2025-01-01T00:00:00+00:00"
    assert funded.evidence_context["current_observed_at"] == "2026-01-01T00:00:00+00:00"

    queued = route_anomalies_to_review_queue(
        anomalies,
        queued_at=datetime(2026, 1, 2, 0, 0),
    )
    assert queued
    assert queued[0].created_at == datetime(2026, 1, 2, 0, 0, tzinfo=UTC)
