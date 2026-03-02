"""Cadence profile extraction from publication history."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from statistics import mean, median, pstdev


@dataclass(frozen=True, slots=True)
class PublicationEvent:
    """Single publication observation for a system/source pair."""

    system_id: str
    source_id: str
    published_at: datetime


@dataclass(frozen=True, slots=True)
class CadenceProfile:
    """Detected cadence profile with confidence metadata."""

    frequency_days: float
    seasonality_months: tuple[int, ...]
    confidence: float
    sample_size: int
    is_sparse: bool


@dataclass(frozen=True, slots=True)
class SourceInventoryObservation:
    """A source/inventory observation used to derive publication signals."""

    system_id: str
    source_id: str
    observed_at: datetime
    published_at: datetime | None = None
    inventory_fingerprint: str | None = None


def _normalized_utc(timestamp: datetime) -> datetime:
    return timestamp.astimezone(UTC)


def extract_publication_events(
    observations: Iterable[SourceInventoryObservation],
) -> list[PublicationEvent]:
    """Extract publication events from source metadata and inventory changes."""
    grouped: dict[tuple[str, str], list[SourceInventoryObservation]] = defaultdict(list)
    for observation in observations:
        grouped[(observation.system_id, observation.source_id)].append(observation)

    extracted: list[PublicationEvent] = []
    for system_id, source_id in sorted(grouped.keys()):
        history = sorted(
            grouped[(system_id, source_id)], key=lambda item: _normalized_utc(item.observed_at)
        )
        seen_publication_times: set[datetime] = set()
        previous_fingerprint: str | None = None

        for item in history:
            explicit_publication = item.published_at is not None
            fingerprint_changed = item.inventory_fingerprint is not None and (
                previous_fingerprint is None or item.inventory_fingerprint != previous_fingerprint
            )
            if explicit_publication:
                publication_time = _normalized_utc(item.published_at or item.observed_at)
            elif fingerprint_changed:
                publication_time = _normalized_utc(item.observed_at)
            else:
                previous_fingerprint = item.inventory_fingerprint
                continue

            if publication_time not in seen_publication_times:
                extracted.append(
                    PublicationEvent(
                        system_id=system_id,
                        source_id=source_id,
                        published_at=publication_time,
                    )
                )
                seen_publication_times.add(publication_time)

            previous_fingerprint = item.inventory_fingerprint

    return extracted


def latest_publications(events: Iterable[PublicationEvent]) -> Mapping[tuple[str, str], datetime]:
    """Return the latest publication timestamp per system/source pair."""
    latest: dict[tuple[str, str], datetime] = {}
    for event in events:
        key = (event.system_id, event.source_id)
        timestamp = _normalized_utc(event.published_at)
        if key not in latest or timestamp > latest[key]:
            latest[key] = timestamp
    return latest


def _intervals_in_days(timestamps: list[datetime]) -> list[float]:
    intervals: list[float] = []
    for index in range(1, len(timestamps)):
        delta = timestamps[index] - timestamps[index - 1]
        intervals.append(delta.total_seconds() / 86400)
    return intervals


def _confidence(intervals: list[float], sample_size: int) -> float:
    if not intervals:
        return 0.25 if sample_size > 0 else 0.0
    if sample_size < 3:
        return round(0.2 + (0.2 * (sample_size / 3.0)), 3)

    interval_mean = mean(intervals)
    if interval_mean <= 0:
        return 0.0
    interval_stdev = pstdev(intervals) if len(intervals) > 1 else 0.0
    coefficient_of_variation = interval_stdev / interval_mean

    stability_score = max(0.0, 1.0 - min(coefficient_of_variation, 1.0))
    sample_score = min(sample_size / 8.0, 1.0)
    confidence = 0.35 + (0.45 * stability_score) + (0.20 * sample_score)
    return round(min(confidence, 0.99), 3)


def build_cadence_profiles(
    events: list[PublicationEvent],
    *,
    sparse_threshold: int = 3,
) -> dict[tuple[str, str], CadenceProfile]:
    """Build deterministic cadence profiles from publication events."""
    grouped: dict[tuple[str, str], list[datetime]] = defaultdict(list)
    for event in events:
        grouped[(event.system_id, event.source_id)].append(_normalized_utc(event.published_at))

    profiles: dict[tuple[str, str], CadenceProfile] = {}
    for key in sorted(grouped.keys()):
        timestamps = sorted(grouped[key])
        intervals = _intervals_in_days(timestamps)
        frequency = float(median(intervals)) if intervals else 365.0
        seasonality_months = tuple(sorted({timestamp.month for timestamp in timestamps}))
        sample_size = len(timestamps)
        is_sparse = sample_size < sparse_threshold
        profiles[key] = CadenceProfile(
            frequency_days=round(frequency, 3),
            seasonality_months=seasonality_months,
            confidence=_confidence(intervals, sample_size),
            sample_size=sample_size,
            is_sparse=is_sparse,
        )

    return profiles
