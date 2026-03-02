"""Route anomaly records into a deterministic review queue."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from pension_data.quality.anomaly_rules import AnomalyRecord, Priority


@dataclass(frozen=True, slots=True)
class ReviewQueueItem:
    """Review-queue envelope for anomaly triage."""

    queue_id: str
    anomaly_id: str
    plan_id: str
    period: str
    priority: Priority
    metric: str
    reason: str
    evidence_context: dict[str, object]
    created_at: datetime


_PRIORITY_ORDER: dict[Priority, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}


def route_anomalies_to_review_queue(
    anomalies: list[AnomalyRecord],
    *,
    queued_at: datetime | None = None,
) -> list[ReviewQueueItem]:
    """Route anomaly records into prioritized review-queue items."""
    created_at = (queued_at or datetime.now(UTC)).astimezone(UTC)
    queue_items: list[ReviewQueueItem] = []

    for anomaly in anomalies:
        if not anomaly.requires_review:
            continue

        queue_items.append(
            ReviewQueueItem(
                queue_id=f"review:{anomaly.anomaly_id}",
                anomaly_id=anomaly.anomaly_id,
                plan_id=anomaly.plan_id,
                period=anomaly.period,
                priority=anomaly.priority,
                metric=anomaly.metric,
                reason=(
                    f"{anomaly.metric} shift {anomaly.shift:.3f} "
                    f"({anomaly.severity}, confidence={anomaly.confidence:.2f})"
                ),
                evidence_context=dict(anomaly.evidence_context),
                created_at=created_at,
            )
        )

    return sorted(
        queue_items,
        key=lambda item: (
            _PRIORITY_ORDER[item.priority],
            item.plan_id,
            item.period,
            item.metric,
            item.anomaly_id,
        ),
    )
