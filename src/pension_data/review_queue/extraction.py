"""Review queue ingestion and state transitions for confidence-routed extraction rows."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime

from pension_data.db.models.review_queue import (
    ExtractionReviewQueueRecord,
    ReviewPriority,
    ReviewQueueAuditEntry,
    ReviewState,
    RoutingOutcome,
)
from pension_data.quality.confidence import ConfidenceRoutingDecision

_ALLOWED_TRANSITIONS: dict[ReviewState, set[ReviewState]] = {
    "new": {"in_review", "resolved", "deferred"},
    "in_review": {"resolved", "deferred"},
    "deferred": {"in_review", "resolved"},
    "resolved": set(),
}

_PRIORITY_ORDER: dict[str, int] = {
    "high": 0,
    "medium": 1,
}


def _normalize_utc(dt: datetime | None) -> datetime:
    current = dt or datetime.now(UTC)
    return current.replace(tzinfo=UTC) if current.tzinfo is None else current.astimezone(UTC)


def build_extraction_review_queue(
    decisions: Sequence[ConfidenceRoutingDecision],
    *,
    queued_at: datetime | None = None,
) -> list[ExtractionReviewQueueRecord]:
    """Persist queue rows for unresolved confidence-routing outcomes."""
    created_at = _normalize_utc(queued_at)
    queue_rows: list[ExtractionReviewQueueRecord] = []
    for decision in decisions:
        if decision.review_priority == "none":
            continue
        routing_outcome: RoutingOutcome
        if decision.routing_outcome == "publish_with_warning":
            routing_outcome = "publish_with_warning"
        elif decision.routing_outcome == "high_priority_review":
            routing_outcome = "high_priority_review"
        else:
            continue
        priority: ReviewPriority
        if decision.review_priority == "high":
            priority = "high"
        elif decision.review_priority == "medium":
            priority = "medium"
        else:
            continue
        queue_id = f"extraction-review:{decision.row_id}"
        audit_entry = ReviewQueueAuditEntry(
            queue_id=queue_id,
            previous_state=None,
            next_state="new",
            actor="system",
            reason=f"routed:{decision.routing_outcome}",
            changed_at=created_at,
        )
        queue_rows.append(
            ExtractionReviewQueueRecord(
                queue_id=queue_id,
                row_id=decision.row_id,
                plan_id=decision.plan_id,
                plan_period=decision.plan_period,
                metric_name=decision.metric_name,
                confidence=decision.confidence,
                routing_outcome=routing_outcome,
                priority=priority,
                state="new",
                created_at=created_at,
                updated_at=created_at,
                evidence_refs=decision.evidence_refs,
                audit_trail=(audit_entry,),
            )
        )

    return sorted(
        queue_rows,
        key=lambda row: (
            _PRIORITY_ORDER[row.priority],
            row.plan_id,
            row.plan_period,
            row.metric_name,
            row.row_id,
        ),
    )


def transition_extraction_review_state(
    queue_rows: Sequence[ExtractionReviewQueueRecord],
    *,
    queue_id: str,
    next_state: ReviewState,
    actor: str,
    reason: str,
    changed_at: datetime | None = None,
) -> list[ExtractionReviewQueueRecord]:
    """Transition one queue row state and append immutable audit trail entry."""
    timestamp = _normalize_utc(changed_at)
    updated_rows: list[ExtractionReviewQueueRecord] = []
    found = False

    for row in queue_rows:
        if row.queue_id != queue_id:
            updated_rows.append(row)
            continue
        found = True
        if next_state not in _ALLOWED_TRANSITIONS[row.state]:
            raise ValueError(f"invalid state transition: {row.state} -> {next_state}")

        audit_entry = ReviewQueueAuditEntry(
            queue_id=row.queue_id,
            previous_state=row.state,
            next_state=next_state,
            actor=actor.strip() or "unknown",
            reason=reason.strip() or "state transition",
            changed_at=timestamp,
        )
        updated_rows.append(
            replace(
                row,
                state=next_state,
                updated_at=timestamp,
                audit_trail=(*row.audit_trail, audit_entry),
            )
        )

    if not found:
        raise ValueError(f"queue row not found for queue_id '{queue_id}'")

    return sorted(
        updated_rows,
        key=lambda row: (
            _PRIORITY_ORDER[row.priority],
            row.plan_id,
            row.plan_period,
            row.metric_name,
            row.row_id,
        ),
    )
