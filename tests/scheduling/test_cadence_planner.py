"""Tests for cadence profile detection and adaptive refresh planner."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from pension_data.scheduling.cadence import (
    PublicationEvent,
    SourceInventoryObservation,
    build_cadence_profiles,
    extract_publication_events,
    latest_publications,
)
from pension_data.scheduling.planner import plan_refresh_windows


def test_extract_publication_events_uses_explicit_publication_timestamps() -> None:
    observations = [
        SourceInventoryObservation(
            "ps-il",
            "official",
            observed_at=datetime(2025, 1, 8, tzinfo=UTC),
            published_at=datetime(2025, 1, 1, tzinfo=UTC),
        ),
        SourceInventoryObservation(
            "ps-il",
            "official",
            observed_at=datetime(2025, 2, 8, tzinfo=UTC),
            published_at=datetime(2025, 2, 1, tzinfo=UTC),
        ),
        SourceInventoryObservation(
            "ps-il",
            "official",
            observed_at=datetime(2025, 2, 12, tzinfo=UTC),
            published_at=datetime(2025, 2, 1, tzinfo=UTC),
        ),
    ]

    events = extract_publication_events(observations)

    assert [event.published_at for event in events] == [
        datetime(2025, 1, 1, tzinfo=UTC),
        datetime(2025, 2, 1, tzinfo=UTC),
    ]


def test_extract_publication_events_uses_inventory_change_when_publication_missing() -> None:
    observations = [
        SourceInventoryObservation(
            "ps-tx",
            "official",
            observed_at=datetime(2025, 1, 10, tzinfo=UTC),
            inventory_fingerprint="a",
        ),
        SourceInventoryObservation(
            "ps-tx",
            "official",
            observed_at=datetime(2025, 2, 10, tzinfo=UTC),
            inventory_fingerprint="a",
        ),
        SourceInventoryObservation(
            "ps-tx",
            "official",
            observed_at=datetime(2025, 3, 10, tzinfo=UTC),
            inventory_fingerprint="b",
        ),
    ]

    events = extract_publication_events(observations)
    profile = build_cadence_profiles(events)[("ps-tx", "official")]

    assert [event.published_at for event in events] == [
        datetime(2025, 1, 10, tzinfo=UTC),
        datetime(2025, 3, 10, tzinfo=UTC),
    ]
    assert profile.frequency_days == 59.0


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
    assert plans[0].next_run_earliest >= datetime(2025, 1, 11, tzinfo=UTC)
    assert plans[0].next_run_latest <= datetime(2025, 2, 3, tzinfo=UTC)


def test_planner_caps_windows_to_max_interval_bound() -> None:
    events = [
        PublicationEvent("ps-ak", "official", datetime(2024, 1, 1, tzinfo=UTC)),
        PublicationEvent("ps-ak", "official", datetime(2025, 1, 1, tzinfo=UTC)),
    ]
    profile = build_cadence_profiles(events)[("ps-ak", "official")]
    last_published = datetime(2025, 1, 1, tzinfo=UTC)
    plans = plan_refresh_windows(
        profiles={("ps-ak", "official"): profile},
        last_publications={("ps-ak", "official"): last_published},
        as_of=datetime(2025, 1, 2, tzinfo=UTC),
        min_interval_days=7.0,
        max_interval_days=30.0,
    )

    assert len(plans) == 1
    plan = plans[0]
    assert plan.recommended_interval_days == 30.0
    assert plan.next_run_latest <= last_published + timedelta(days=30)


def test_planner_rejects_invalid_interval_bounds() -> None:
    profile = build_cadence_profiles(
        [PublicationEvent("ps-il", "official", datetime(2025, 1, 1, tzinfo=UTC))]
    )[("ps-il", "official")]
    with pytest.raises(ValueError, match="min_interval_days"):
        plan_refresh_windows(
            profiles={("ps-il", "official"): profile},
            last_publications={("ps-il", "official"): datetime(2025, 1, 1, tzinfo=UTC)},
            as_of=datetime(2025, 1, 2, tzinfo=UTC),
            min_interval_days=31.0,
            max_interval_days=30.0,
        )


def test_planner_output_is_deterministic_for_sparse_and_seasonal_history() -> None:
    sparse_events = [
        PublicationEvent("ps-wa", "official", datetime(2024, 1, 10, tzinfo=UTC)),
        PublicationEvent("ps-wa", "official", datetime(2025, 1, 9, tzinfo=UTC)),
    ]
    seasonal_events = [
        PublicationEvent("ps-ny", "official", datetime(2023, 3, 10, tzinfo=UTC)),
        PublicationEvent("ps-ny", "official", datetime(2023, 9, 10, tzinfo=UTC)),
        PublicationEvent("ps-ny", "official", datetime(2024, 3, 10, tzinfo=UTC)),
        PublicationEvent("ps-ny", "official", datetime(2024, 9, 10, tzinfo=UTC)),
    ]
    profiles = build_cadence_profiles(sparse_events + seasonal_events)
    last_publications = latest_publications(sparse_events + seasonal_events)
    as_of = datetime(2025, 1, 20, tzinfo=UTC)
    plans = plan_refresh_windows(
        profiles=profiles,
        last_publications=last_publications,
        as_of=as_of,
        min_interval_days=7.0,
        max_interval_days=120.0,
    )

    observed = {
        (plan.system_id, plan.source_id): (
            plan.recommended_interval_days,
            plan.next_run_earliest,
            plan.next_run_latest,
        )
        for plan in plans
    }

    assert observed == {
        ("ps-ny", "official"): (
            120.0,
            datetime(2025, 1, 20, tzinfo=UTC),
            datetime(2025, 1, 20, tzinfo=UTC),
        ),
        ("ps-wa", "official"): (
            120.0,
            datetime(2025, 4, 8, 23, 42, 43, 200000, tzinfo=UTC),
            datetime(2025, 5, 9, 0, 0, tzinfo=UTC),
        ),
    }


def test_planner_adapts_to_detected_seasonality() -> None:
    observations = [
        SourceInventoryObservation(
            "ps-ny",
            "official",
            observed_at=datetime(2023, 3, 11, tzinfo=UTC),
            published_at=datetime(2023, 3, 10, tzinfo=UTC),
            inventory_fingerprint="m1",
        ),
        SourceInventoryObservation(
            "ps-ny",
            "official",
            observed_at=datetime(2023, 9, 12, tzinfo=UTC),
            published_at=datetime(2023, 9, 10, tzinfo=UTC),
            inventory_fingerprint="m2",
        ),
        SourceInventoryObservation(
            "ps-ny",
            "official",
            observed_at=datetime(2024, 3, 11, tzinfo=UTC),
            published_at=datetime(2024, 3, 10, tzinfo=UTC),
            inventory_fingerprint="m3",
        ),
        SourceInventoryObservation(
            "ps-ny",
            "official",
            observed_at=datetime(2024, 9, 11, tzinfo=UTC),
            published_at=datetime(2024, 9, 10, tzinfo=UTC),
            inventory_fingerprint="m4",
        ),
    ]
    events = extract_publication_events(observations)
    profile = build_cadence_profiles(events)[("ps-ny", "official")]
    as_of = datetime(2025, 4, 15, tzinfo=UTC)
    plans = plan_refresh_windows(
        profiles={("ps-ny", "official"): profile},
        last_publications=latest_publications(events),
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
