"""Confidence routing policy for extraction outputs."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from pension_data.finite_guards import is_finite_number

RoutingOutcome = Literal["auto_accept", "publish_with_warning", "high_priority_review"]
ReviewPriority = Literal["none", "medium", "high"]

AUTO_ACCEPT_THRESHOLD = 0.90
WARNING_QUEUE_THRESHOLD = 0.75


@dataclass(frozen=True, slots=True)
class ExtractionConfidenceInput:
    """Confidence payload for a persisted extraction metric row."""

    row_id: str
    plan_id: str
    plan_period: str
    metric_name: str
    confidence: float
    evidence_refs: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ConfidenceRoutingDecision:
    """Routing decision derived from extraction confidence thresholds."""

    row_id: str
    plan_id: str
    plan_period: str
    metric_name: str
    confidence: float
    routing_outcome: RoutingOutcome
    review_priority: ReviewPriority
    publish_blocked: bool
    evidence_refs: tuple[str, ...]


def _bounded_confidence(value: float) -> float | None:
    """Clamp to [0, 1]; return None for a non-finite (NaN/inf) confidence.

    A bare ``max(0.0, min(1.0, value))`` returns 1.0 for NaN (min(1.0, nan) == 1.0),
    which would auto-accept a garbage extraction. None signals "not a trustworthy
    confidence" so the caller routes it to review instead.
    """
    if not is_finite_number(value):
        return None
    return round(max(0.0, min(1.0, value)), 6)


def route_confidence_row(row: ExtractionConfidenceInput) -> ConfidenceRoutingDecision:
    """Route one extraction row according to approved confidence policy."""
    bounded = _bounded_confidence(row.confidence)
    if bounded is None:
        # Non-finite confidence (NaN/inf) is never trusted: force high-priority review
        # and block publication rather than letting the clamp report a false 1.0.
        return ConfidenceRoutingDecision(
            row_id=row.row_id,
            plan_id=row.plan_id,
            plan_period=row.plan_period,
            metric_name=row.metric_name,
            confidence=0.0,
            routing_outcome="high_priority_review",
            review_priority="high",
            publish_blocked=True,
            evidence_refs=row.evidence_refs,
        )
    confidence = bounded
    if confidence >= AUTO_ACCEPT_THRESHOLD:
        routing_outcome: RoutingOutcome = "auto_accept"
        review_priority: ReviewPriority = "none"
    elif confidence >= WARNING_QUEUE_THRESHOLD:
        routing_outcome = "publish_with_warning"
        review_priority = "medium"
    else:
        routing_outcome = "high_priority_review"
        review_priority = "high"

    return ConfidenceRoutingDecision(
        row_id=row.row_id,
        plan_id=row.plan_id,
        plan_period=row.plan_period,
        metric_name=row.metric_name,
        confidence=confidence,
        routing_outcome=routing_outcome,
        review_priority=review_priority,
        publish_blocked=False,
        evidence_refs=row.evidence_refs,
    )


def route_confidence_rows(
    rows: Sequence[ExtractionConfidenceInput],
) -> list[ConfidenceRoutingDecision]:
    """Route extraction rows with deterministic ordering."""
    decisions = [route_confidence_row(row) for row in rows]
    return sorted(
        decisions,
        key=lambda row: (
            row.plan_id,
            row.plan_period,
            row.metric_name,
            row.row_id,
        ),
    )
