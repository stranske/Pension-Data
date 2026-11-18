"""Tests for anomaly rules and review-queue routing."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from pension_data.quality.anomaly_rules import AnomalyThresholds, TimeSeriesPoint, detect_anomalies
from pension_data.review_queue.anomalies import route_anomalies_to_review_queue


def test_fiscal_two_digit_year_sorts_after_prior_four_digit_year() -> None:
    points = [
        TimeSeriesPoint(
            plan_id="fiscal-order",
            period=period,
            observed_at=datetime(2025, 1, 1, tzinfo=UTC),
            funded_ratio=ratio,
            allocations={},
            confidence=0.9,
            evidence_refs=(f"doc:{period}",),
            provenance={},
        )
        for period, ratio in [("FY20", 0.6), ("FY2019", 0.8)]
    ]
    anomalies = detect_anomalies(points)
    assert len(anomalies) == 1
    assert anomalies[0].period == "FY20"
    assert anomalies[0].evidence_context["previous_period"] == "FY2019"
    assert anomalies[0].evidence_context["metric_evidence"]["signed_delta"] == -0.2


def test_numbered_periods_sort_chronologically_for_anomaly_evidence() -> None:
    points = [
        _point(
            plan_id="numbered-periods",
            period=period,
            observed_at=datetime(2025, 1, 1, tzinfo=UTC),
            funded_ratio=ratio,
            equity=0.4,
            fixed_income=0.3,
            confidence=0.9,
        )
        for period, ratio in [("period 10", 0.6), ("period 9", 0.8)]
    ]

    anomalies = detect_anomalies(points)

    assert len(anomalies) == 1
    assert anomalies[0].period == "period 10"
    assert anomalies[0].evidence_context["previous_period"] == "period 9"
    assert anomalies[0].evidence_context["metric_evidence"]["signed_delta"] == -0.2


@pytest.mark.parametrize("missing_index", [0, 1])
def test_missing_funded_ratio_preserves_allocation_detection(missing_index: int) -> None:
    points = [
        TimeSeriesPoint(
            plan_id="partial",
            period=str(2024 + index),
            observed_at=datetime(2025, 1, 1, tzinfo=UTC),
            funded_ratio=None if index == missing_index else 0.8,
            allocations={"public_equity": allocation},
            confidence=0.9,
            evidence_refs=(f"doc:{index}",),
            provenance={},
        )
        for index, allocation in enumerate([0.4, 0.6])
    ]
    anomalies = detect_anomalies(points)
    assert len(anomalies) == 1
    assert anomalies[0].metric == "allocation:public_equity"
    assert anomalies[0].period == "2025"


@pytest.mark.parametrize(
    "name",
    [
        "funded_shift_warning",
        "funded_shift_critical",
        "allocation_shift_warning",
        "allocation_shift_critical",
        "min_confidence_for_medium_priority",
    ],
)
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_anomaly_thresholds_reject_non_finite_values(name: str, value: float) -> None:
    with pytest.raises(ValueError, match=rf"{name} must be within \[0.0, 1.0\]"):
        AnomalyThresholds(**{name: value})


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


@pytest.mark.parametrize("weak_index", [0, 1], ids=["weak-previous", "weak-current"])
@pytest.mark.parametrize(
    ("shift", "severity", "priority", "score"),
    [(0.1875, "warning", "low", 0.3125), (0.25, "critical", "medium", 0.625)],
)
def test_weaker_observation_controls_anomaly_confidence_and_triage(
    weak_index: int, shift: float, severity: str, priority: str, score: float
) -> None:
    points = [
        _point(
            plan_id="mixed-confidence",
            period=str(2024 + index),
            observed_at=datetime(2025 + index, 1, 1, tzinfo=UTC),
            funded_ratio=0.75 - index * shift,
            equity=0.25 + index * shift,
            fixed_income=0.25,
            confidence=0.25 if index == weak_index else 1.0,
        )
        for index in range(2)
    ]

    anomalies = detect_anomalies(
        points,
        thresholds=AnomalyThresholds(
            funded_shift_warning=0.125,
            funded_shift_critical=0.25,
            allocation_shift_warning=0.125,
            allocation_shift_critical=0.25,
            min_confidence_for_medium_priority=0.5,
        ),
    )

    assert {item.metric for item in anomalies} == {"funded_ratio", "allocation:public_equity"}
    for anomaly in anomalies:
        assert anomaly.confidence == 0.25
        assert anomaly.severity == severity
        assert anomaly.priority == priority
        assert anomaly.score == score

    queued = route_anomalies_to_review_queue(anomalies, queued_at=datetime(2026, 1, 2, tzinfo=UTC))
    assert len(queued) == 2
    for item in queued:
        assert item.priority == priority
        assert f"({severity}, confidence=0.25)" in item.reason


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


@pytest.mark.parametrize("missing_index", [0, 1], ids=["new-class", "removed-class"])
def test_allocation_class_changes_preserve_shift_direction_and_evidence(missing_index: int) -> None:
    points = [
        TimeSeriesPoint(
            plan_id="allocation-change",
            period=str(2024 + index),
            observed_at=datetime(2025 + index, 1, 1, tzinfo=UTC),
            funded_ratio=0.75,
            allocations={"fixed_income": 0.25}
            | ({} if index == missing_index else {"private_equity": 0.25}),
            confidence=0.75,
            evidence_refs=(f"doc:{index}",),
            provenance={"table": f"allocations-{index}"},
        )
        for index in range(2)
    ]

    anomalies = detect_anomalies(
        list(reversed(points)),
        thresholds=AnomalyThresholds(
            allocation_shift_warning=0.125,
            allocation_shift_critical=0.25,
        ),
    )

    assert len(anomalies) == 1
    anomaly = anomalies[0]
    assert anomaly.anomaly_id == "allocation-change:2025:allocation_shift:private_equity"
    assert anomaly.metric == "allocation:private_equity"
    assert anomaly.shift == 0.25
    assert anomaly.severity == "critical"
    assert anomaly.priority == "high"
    assert anomaly.score == 0.875
    assert anomaly.evidence_context == {
        "previous_period": "2024",
        "current_period": "2025",
        "previous_observed_at": "2025-01-01T00:00:00+00:00",
        "current_observed_at": "2026-01-01T00:00:00+00:00",
        "evidence_refs": ["doc:0", "doc:1"],
        "previous_provenance": {"table": "allocations-0"},
        "current_provenance": {"table": "allocations-1"},
        "metric_evidence": {
            "metric": "allocation:private_equity",
            "previous_value": 0.0 if missing_index == 0 else 0.25,
            "current_value": 0.25 if missing_index == 0 else 0.0,
            "signed_delta": 0.25 if missing_index == 0 else -0.25,
            "absolute_delta": 0.25,
            "thresholds": {"warning": 0.125, "critical": 0.25},
        },
    }


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


@pytest.mark.parametrize("metric", ["funded_ratio", "allocation:public_equity"])
@pytest.mark.parametrize(
    ("shift", "critical", "severity", "priority", "score"),
    [
        pytest.param(0.125, 0.25, "warning", "medium", 0.0, id="at-warning"),
        pytest.param(0.25, 0.25, "critical", "high", 0.75, id="at-critical"),
        pytest.param(0.125, 0.125, "critical", "high", 0.75, id="equal-thresholds"),
    ],
)
def test_exact_threshold_shifts_preserve_severity_priority_and_score(
    metric: str,
    shift: float,
    critical: float,
    severity: str,
    priority: str,
    score: float,
) -> None:
    # Binary-exact fractions isolate inclusive threshold behavior from rounding.
    points = [
        _point(
            plan_id="threshold-boundary",
            period=str(2024 + index),
            observed_at=datetime(2025, 1, 1, tzinfo=UTC),
            funded_ratio=0.5 - index * shift if metric == "funded_ratio" else 0.5,
            equity=0.5 - index * shift if metric == "allocation:public_equity" else 0.5,
            fixed_income=0.25,
            confidence=0.5,
        )
        for index in range(2)
    ]

    anomalies = detect_anomalies(
        points,
        thresholds=AnomalyThresholds(
            funded_shift_warning=0.125,
            funded_shift_critical=critical,
            allocation_shift_warning=0.125,
            allocation_shift_critical=critical,
            min_confidence_for_medium_priority=0.5,
        ),
    )

    assert len(anomalies) == 1
    anomaly = anomalies[0]
    assert anomaly.metric == metric
    assert anomaly.shift == shift
    assert anomaly.severity == severity
    assert anomaly.priority == priority
    assert anomaly.score == score
    assert anomaly.requires_review is True
    queued = route_anomalies_to_review_queue(anomalies, queued_at=datetime(2025, 1, 2, tzinfo=UTC))
    assert len(queued) == 1
    assert queued[0].priority == priority


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


def test_detect_anomalies_uses_numeric_period_order_for_fiscal_labels() -> None:
    points = [
        _point(
            plan_id="fiscal-order",
            period="FY10",
            observed_at=datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
            funded_ratio=0.65,
            equity=0.60,
            fixed_income=0.20,
            confidence=0.95,
        ),
        _point(
            plan_id="fiscal-order",
            period="FY9",
            observed_at=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            funded_ratio=0.80,
            equity=0.45,
            fixed_income=0.35,
            confidence=0.95,
        ),
    ]

    anomalies = detect_anomalies(points)
    assert anomalies
    funded = [item for item in anomalies if item.metric == "funded_ratio"][0]
    assert funded.period == "FY10"
    assert funded.evidence_context["previous_period"] == "FY9"
    assert funded.evidence_context["current_period"] == "FY10"


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


@pytest.mark.parametrize("aware_index", [0, 1], ids=["aware-previous", "aware-current"])
def test_same_period_observations_with_mixed_timezones_sort_by_utc(aware_index: int) -> None:
    points = [
        _point(
            plan_id="mixed-timezones",
            period="2025",
            observed_at=(
                datetime(2025, 1, 1, 4 + index, tzinfo=timezone(timedelta(hours=4)))
                if index == aware_index
                else datetime(2025, 1, 1, index)
            ),
            funded_ratio=0.75 - index * 0.25,
            equity=0.5,
            fixed_income=0.25,
            confidence=0.9,
        )
        for index in range(2)
    ]
    # Reverse arrival order so stable input ordering cannot hide the timestamp tie-breaker.
    anomalies = detect_anomalies(list(reversed(points)))

    assert len(anomalies) == 1
    anomaly = anomalies[0]
    assert anomaly.metric == "funded_ratio"
    assert anomaly.evidence_context["previous_observed_at"] == "2025-01-01T00:00:00+00:00"
    assert anomaly.evidence_context["current_observed_at"] == "2025-01-01T01:00:00+00:00"
    assert anomaly.evidence_context["metric_evidence"]["signed_delta"] == -0.25


class _NoOffsetTZ(tzinfo):
    def utcoffset(self, dt: datetime | None) -> None:
        return None


@pytest.mark.parametrize(
    ("earlier", "later", "earlier_iso", "later_iso"),
    [
        (
            datetime(2025, 1, 1, tzinfo=_NoOffsetTZ()),
            datetime(2025, 1, 1, 1, tzinfo=UTC),
            "2025-01-01T00:00:00+00:00",
            "2025-01-01T01:00:00+00:00",
        ),
        (
            datetime(1, 1, 1, tzinfo=timezone(timedelta(hours=1))),
            datetime(1, 1, 1, tzinfo=UTC),
            "0001-01-01T00:00:00+01:00",
            "0001-01-01T00:00:00+00:00",
        ),
        (
            pd.Timestamp(1735689600000000001, unit="ns", tz=UTC),
            pd.Timestamp(1735689600000000002, unit="ns", tz=UTC),
            "2025-01-01T00:00:00.000000001+00:00",
            "2025-01-01T00:00:00.000000002+00:00",
        ),
    ],
    ids=["no-offset", "utc-underflow", "pandas-nanosecond"],
)
def test_same_period_observations_with_edge_case_utc_offsets(
    monkeypatch: pytest.MonkeyPatch,
    earlier: datetime,
    later: datetime,
    earlier_iso: str,
    later_iso: str,
) -> None:
    points = [
        _point(
            plan_id="offset-edge-case",
            period="FY2025",
            observed_at=observed_at,
            funded_ratio=funded_ratio,
            equity=0.5,
            fixed_income=0.25,
            confidence=0.9,
        )
        for observed_at, funded_ratio in [(later, 0.5), (earlier, 0.75)]
    ]

    try:
        with monkeypatch.context() as mp:
            mp.setenv("TZ", "America/New_York")
            if hasattr(time, "tzset"):
                time.tzset()
            (anomaly,) = detect_anomalies(points)
    finally:
        if hasattr(time, "tzset"):
            time.tzset()

    assert anomaly.evidence_context["previous_observed_at"] == earlier_iso
    assert anomaly.evidence_context["current_observed_at"] == later_iso
    assert anomaly.evidence_context["metric_evidence"]["signed_delta"] == -0.25


@pytest.mark.parametrize(
    "observed_at",
    [
        datetime(1, 1, 1, tzinfo=timezone(timedelta(hours=1))),
        pd.Timestamp(1735689600000000000, unit="ns", tz=UTC),
    ],
    ids=["utc-underflow", "pandas-nanosecond"],
)
def test_single_point_edge_case_timestamps_return_no_anomalies(observed_at: datetime) -> None:
    point = _point(
        plan_id="single-edge-timestamp",
        period="FY2025",
        observed_at=observed_at,
        funded_ratio=0.75,
        equity=0.5,
        fixed_income=0.25,
        confidence=0.9,
    )

    assert detect_anomalies([point]) == []


@pytest.mark.parametrize(
    ("previous_minute", "current_minute"),
    [(45, 15), (30, 30)],
    ids=["wall-clock-rollback", "repeated-wall-time"],
)
def test_same_period_observations_across_dst_fold_sort_by_utc(
    previous_minute: int, current_minute: int
) -> None:
    eastern = ZoneInfo("America/New_York")
    previous = _point(
        plan_id="dst-fold",
        period="2025",
        observed_at=datetime(2025, 11, 2, 1, previous_minute, tzinfo=eastern, fold=0),
        funded_ratio=0.75,
        equity=0.5,
        fixed_income=0.25,
        confidence=0.9,
    )
    current = _point(
        plan_id="dst-fold",
        period="2025",
        observed_at=datetime(2025, 11, 2, 1, current_minute, tzinfo=eastern, fold=1),
        funded_ratio=0.5,
        equity=0.5,
        fixed_income=0.25,
        confidence=0.9,
    )

    anomalies = detect_anomalies([current, previous])

    assert len(anomalies) == 1
    anomaly = anomalies[0]
    assert anomaly.metric == "funded_ratio"
    assert anomaly.shift == 0.25
    assert anomaly.evidence_context["previous_observed_at"] == (
        f"2025-11-02T05:{previous_minute:02d}:00+00:00"
    )
    assert anomaly.evidence_context["current_observed_at"] == (
        f"2025-11-02T06:{current_minute:02d}:00+00:00"
    )
    assert anomaly.evidence_context["metric_evidence"]["signed_delta"] == -0.25
    assert previous.observed_at.tzinfo is eastern
    assert previous.observed_at.fold == 0
    assert current.observed_at.tzinfo is eastern
    assert current.observed_at.fold == 1


@pytest.mark.parametrize("corrupt_metric", ["funded_ratio", "allocation:public_equity"])
@pytest.mark.parametrize("corrupt_index", [0, 1], ids=["previous", "current"])
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_metric_preserves_other_material_shifts(
    corrupt_metric: str, corrupt_index: int, value: float
) -> None:
    points = [
        _point(
            plan_id="partially-corrupt",
            period=str(2024 + index),
            observed_at=datetime(2025, 1, 1, tzinfo=UTC),
            funded_ratio=(
                value
                if corrupt_metric == "funded_ratio" and index == corrupt_index
                else 0.75 - index * 0.25
            ),
            equity=(
                value
                if corrupt_metric == "allocation:public_equity" and index == corrupt_index
                else 0.25 + index * 0.25
            ),
            fixed_income=0.25,
            confidence=0.9,
        )
        for index in range(2)
    ]

    anomalies = detect_anomalies(
        points,
        thresholds=AnomalyThresholds(
            funded_shift_warning=0.125,
            funded_shift_critical=0.25,
            allocation_shift_warning=0.125,
            allocation_shift_critical=0.25,
        ),
    )

    expected_metric = (
        "allocation:public_equity" if corrupt_metric == "funded_ratio" else "funded_ratio"
    )
    assert [anomaly.metric for anomaly in anomalies] == [expected_metric]
    anomaly = anomalies[0]
    assert anomaly.period == "2025"
    assert anomaly.shift == 0.25
    assert anomaly.score == 0.95
    assert anomaly.severity == "critical"
    assert anomaly.priority == "high"
    assert anomaly.evidence_context["metric_evidence"]["signed_delta"] == (
        0.25 if corrupt_metric == "funded_ratio" else -0.25
    )


@pytest.mark.parametrize("include_history", [False, True], ids=["single-points", "with-history"])
def test_finite_observations_compare_only_within_the_same_plan(include_history: bool) -> None:
    points = [
        _point(
            plan_id=plan_id,
            period=str(2024 + index),
            observed_at=datetime(2025 + index, 1, 1, tzinfo=UTC),
            funded_ratio=baseline - index * 0.25,
            equity=0.5,
            fixed_income=0.25,
            confidence=0.9,
        )
        for index in range(2 if include_history else 1)
        for plan_id, baseline in [("first", 0.875), ("second", 0.375)]
    ]

    anomalies = detect_anomalies(list(reversed(points)))

    assert [(item.plan_id, item.period, item.metric) for item in anomalies] == (
        [("first", "2025", "funded_ratio"), ("second", "2025", "funded_ratio")]
        if include_history
        else []
    )
    for anomaly in anomalies:
        assert anomaly.shift == 0.25
        assert anomaly.evidence_context["metric_evidence"]["signed_delta"] == -0.25
        assert anomaly.evidence_context["evidence_refs"] == [
            f"doc:{anomaly.plan_id}:2024",
            f"doc:{anomaly.plan_id}:2025",
        ]


def test_anomaly_detection_skips_nonfinite_values_and_unrelated_plan_transitions() -> None:
    """Corrupt observations and plan boundaries must not create false anomalies."""
    points = [
        _point(
            plan_id="first",
            period="FY",
            observed_at=datetime(2025, 1, 1),
            funded_ratio=float("nan"),
            equity=float("inf"),
            fixed_income=0.3,
            confidence=0.9,
        ),
        _point(
            plan_id="second",
            period="no-date",
            observed_at=datetime(2025, 2, 1),
            funded_ratio=0.1,
            equity=0.1,
            fixed_income=0.3,
            confidence=0.9,
        ),
        _point(
            plan_id="second",
            period="later",
            observed_at=datetime(2025, 3, 1),
            funded_ratio=float("inf"),
            equity=float("nan"),
            fixed_income=0.3,
            confidence=0.9,
        ),
    ]

    assert detect_anomalies(points) == []
