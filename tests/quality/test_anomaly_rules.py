"""Tests for anomaly rules and review-queue routing."""

from __future__ import annotations

from datetime import UTC, datetime

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
    assert funded[0].evidence_context["previous_period"] == "2024"
    assert funded[0].evidence_context["current_period"] == "2025"


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

    anomalies = detect_anomalies(points, thresholds=AnomalyThresholds(allocation_shift_warning=0.08))
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
