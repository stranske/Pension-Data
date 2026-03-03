"""Review queue persistence models for confidence-routed extraction rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

RoutingOutcome = Literal["publish_with_warning", "high_priority_review"]
ReviewPriority = Literal["medium", "high"]
ReviewState = Literal["new", "in_review", "resolved", "deferred"]


@dataclass(frozen=True, slots=True)
class ReviewQueueAuditEntry:
    """Immutable audit log entry for reviewer-state transitions."""

    queue_id: str
    previous_state: ReviewState | None
    next_state: ReviewState
    actor: str
    reason: str
    changed_at: datetime


@dataclass(frozen=True, slots=True)
class ExtractionReviewQueueRecord:
    """Persisted review-queue row for unresolved confidence-routed extraction outputs."""

    queue_id: str
    row_id: str
    plan_id: str
    plan_period: str
    metric_name: str
    confidence: float
    routing_outcome: RoutingOutcome
    priority: ReviewPriority
    state: ReviewState
    created_at: datetime
    updated_at: datetime
    evidence_refs: tuple[str, ...]
    audit_trail: tuple[ReviewQueueAuditEntry, ...]
