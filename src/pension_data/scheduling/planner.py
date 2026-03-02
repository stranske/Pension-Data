"""Adaptive refresh window planner based on cadence profiles."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from pension_data.scheduling.cadence import CadenceProfile


@dataclass(frozen=True, slots=True)
class RefreshPlan:
    """Next-run planning window for a system/source pair."""

    system_id: str
    source_id: str
    recommended_interval_days: float
    next_run_earliest: datetime
    next_run_latest: datetime
    profile_confidence: float


def _clamp(value: float, *, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _with_seasonality_anchor(
    *,
    target: datetime,
    as_of: datetime,
    seasonality_months: tuple[int, ...],
) -> datetime:
    if not seasonality_months:
        return target
    if target.month in seasonality_months:
        return target

    base = max(target, as_of)
    candidates: list[datetime] = []
    for month in seasonality_months:
        year = base.year if month >= base.month else base.year + 1
        candidates.append(datetime(year, month, 1, tzinfo=UTC))
    return min(candidates)


def plan_refresh_windows(
    *,
    profiles: Mapping[tuple[str, str], CadenceProfile],
    last_publications: Mapping[tuple[str, str], datetime],
    as_of: datetime,
    min_interval_days: float = 7.0,
    max_interval_days: float = 120.0,
) -> list[RefreshPlan]:
    """Plan adaptive refresh windows with confidence and interval safeguards."""
    as_of_utc = as_of.astimezone(UTC)
    plans: list[RefreshPlan] = []

    for key in sorted(profiles.keys()):
        profile = profiles[key]
        system_id, source_id = key
        if key not in last_publications:
            continue

        last_seen = last_publications[key].astimezone(UTC)
        bounded_interval_days = _clamp(
            profile.frequency_days,
            minimum=min_interval_days,
            maximum=max_interval_days,
        )
        target = last_seen + timedelta(days=bounded_interval_days)
        target = _with_seasonality_anchor(
            target=target,
            as_of=as_of_utc,
            seasonality_months=profile.seasonality_months,
        )
        if target < as_of_utc and profile.seasonality_months:
            target = _with_seasonality_anchor(
                target=as_of_utc,
                as_of=as_of_utc,
                seasonality_months=profile.seasonality_months,
            )

        jitter_days = bounded_interval_days * (0.05 + ((1.0 - profile.confidence) * 0.30))
        earliest = max(as_of_utc, target - timedelta(days=jitter_days))
        latest = max(earliest, target + timedelta(days=jitter_days))

        plans.append(
            RefreshPlan(
                system_id=system_id,
                source_id=source_id,
                recommended_interval_days=round(bounded_interval_days, 3),
                next_run_earliest=earliest,
                next_run_latest=latest,
                profile_confidence=profile.confidence,
            )
        )

    return plans
