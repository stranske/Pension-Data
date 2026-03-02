"""Tests for cadence profile detection and adaptive refresh planner."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pension_data.scheduling.cadence import PublicationEvent, build_cadence_profiles
from pension_data.scheduling.planner import plan_refresh_windows


def test_cadence_profile_detects_regular_monthly_pattern() -> None:
    events = [
        PublicationEvent("ps-il", "official", datetime(2025, month, 1, tzinfo=UTC))
        for month in (1, 2, 3, 4, 5, 6)
    ]
    profile = build_cadence_profiles(events)[("ps-il", "official")]

    assert profile.frequency_days == 31.0
    assert profile.confidence >= 0.8
    assert profile.sample_size == 6
    assert profile.seasonality_months == (1, 2, 3, 4, 5, 6)


def test_cadence_profile_marks_sparse_history_with_lower_confidence() -> None:
    events = [
        PublicationEvent("ps-ak", "official", datetime(2024, 7, 1, tzinfo=UTC)),
        PublicationEvent("ps-ak", "official", datetime(2025, 7, 1, tzinfo=UTC)),
    ]
    profile = build_cadence_profiles(events)[("ps-ak", "official")]

    assert profile.is_sparse
    assert profile.confidence < 0.7
    assert profile.frequency_days == 365.0


def test_cadence_profile_captures_seasonal_publication_months() -> None:
    events = [
        PublicationEvent("ps-tx", "official", datetime(2023, 3, 15, tzinfo=UTC)),
        PublicationEvent("ps-tx", "official", datetime(2023, 9, 20, tzinfo=UTC)),
        PublicationEvent("ps-tx", "official", datetime(2024, 3, 10, tzinfo=UTC)),
        PublicationEvent("ps-tx", "official", datetime(2024, 9, 18, tzinfo=UTC)),
    ]
    profile = build_cadence_profiles(events)[("ps-tx", "official")]

    assert profile.seasonality_months == (3, 9)
    assert profile.frequency_days > 170


def test_planner_enforces_interval_bounds() -> None:
    events = [
        PublicationEvent("ps-hi", "official", datetime(2025, 1, 1, tzinfo=UTC)),
        PublicationEvent("ps-hi", "official", datetime(2025, 1, 2, tzinfo=UTC)),
        PublicationEvent("ps-hi", "official", datetime(2025, 1, 3, tzinfo=UTC)),
        PublicationEvent("ps-hi", "official", datetime(2025, 1, 4, tzinfo=UTC)),
    ]
    profile = build_cadence_profiles(events)[("ps-hi", "official")]
    plans = plan_refresh_windows(
        profiles={("ps-hi", "official"): profile},
        last_publications={("ps-hi", "official"): datetime(2025, 1, 4, tzinfo=UTC)},
        as_of=datetime(2025, 1, 5, tzinfo=UTC),
        min_interval_days=7.0,
        max_interval_days=30.0,
    )

    assert len(plans) == 1
    assert plans[0].recommended_interval_days == 7.0


def test_planner_adapts_to_detected_seasonality() -> None:
    events = [
        PublicationEvent("ps-ny", "official", datetime(2023, 3, 10, tzinfo=UTC)),
        PublicationEvent("ps-ny", "official", datetime(2023, 9, 10, tzinfo=UTC)),
        PublicationEvent("ps-ny", "official", datetime(2024, 3, 10, tzinfo=UTC)),
        PublicationEvent("ps-ny", "official", datetime(2024, 9, 10, tzinfo=UTC)),
    ]
    profile = build_cadence_profiles(events)[("ps-ny", "official")]
    as_of = datetime(2025, 4, 15, tzinfo=UTC)
    plans = plan_refresh_windows(
        profiles={("ps-ny", "official"): profile},
        last_publications={("ps-ny", "official"): datetime(2024, 9, 10, tzinfo=UTC)},
        as_of=as_of,
        min_interval_days=7.0,
        max_interval_days=365.0,
    )

    assert len(plans) == 1
    plan = plans[0]
    assert plan.next_run_earliest.month in (8, 9)
    assert plan.next_run_latest.month == 9
    assert plan.next_run_latest >= plan.next_run_earliest
    assert plan.next_run_earliest >= as_of
    assert plan.next_run_latest - plan.next_run_earliest <= timedelta(days=120)
