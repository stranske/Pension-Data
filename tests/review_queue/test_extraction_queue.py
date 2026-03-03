"""Tests for extraction confidence queue ingestion and state transitions."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pension_data.quality.confidence import (
    ConfidenceRoutingDecision,
    ExtractionConfidenceInput,
    route_confidence_rows,
)
from pension_data.review_queue.extraction import (
    build_extraction_review_queue,
    transition_extraction_review_state,
)


def _decisions() -> list[ConfidenceRoutingDecision]:
    return route_confidence_rows(
        [
            ExtractionConfidenceInput(
                row_id="row:auto",
                plan_id="ca-pers",
                plan_period="FY2025",
                metric_name="funded_ratio",
                confidence=0.94,
                evidence_refs=("p.40",),
            ),
            ExtractionConfidenceInput(
                row_id="row:warn",
                plan_id="ca-pers",
                plan_period="FY2025",
                metric_name="discount_rate",
                confidence=0.80,
                evidence_refs=("p.41",),
            ),
            ExtractionConfidenceInput(
                row_id="row:review",
                plan_id="ca-pers",
                plan_period="FY2025",
                metric_name="manager_fee_rate",
                confidence=0.60,
                evidence_refs=("p.42",),
            ),
        ]
    )


def test_queue_ingestion_includes_medium_and_high_priority_items_only() -> None:
    rows = build_extraction_review_queue(
        _decisions(),
        queued_at=datetime(2026, 1, 2, tzinfo=UTC),
    )

    assert len(rows) == 2
    assert [row.priority for row in rows] == ["high", "medium"]
    assert {row.row_id for row in rows} == {"row:warn", "row:review"}
    assert all(row.state == "new" for row in rows)
    assert all(row.audit_trail[0].next_state == "new" for row in rows)
    assert all(row.audit_trail[0].actor == "system" for row in rows)


def test_low_confidence_item_is_high_priority_in_queue() -> None:
    rows = build_extraction_review_queue(_decisions())
    high_priority = [row for row in rows if row.priority == "high"]

    assert len(high_priority) == 1
    assert high_priority[0].row_id == "row:review"
    assert high_priority[0].routing_outcome == "high_priority_review"


def test_state_transitions_append_audit_entries() -> None:
    queued = build_extraction_review_queue(
        _decisions(),
        queued_at=datetime(2026, 1, 2, tzinfo=UTC),
    )
    target = [row for row in queued if row.row_id == "row:warn"][0]

    in_review = transition_extraction_review_state(
        queued,
        queue_id=target.queue_id,
        next_state="in_review",
        actor="reviewer-a",
        reason="triage started",
        changed_at=datetime(2026, 1, 3, tzinfo=UTC),
    )
    resolved = transition_extraction_review_state(
        in_review,
        queue_id=target.queue_id,
        next_state="resolved",
        actor="reviewer-a",
        reason="validated by source evidence",
        changed_at=datetime(2026, 1, 4, tzinfo=UTC),
    )
    updated_target = [row for row in resolved if row.queue_id == target.queue_id][0]

    assert updated_target.state == "resolved"
    assert len(updated_target.audit_trail) == 3
    assert updated_target.audit_trail[1].previous_state == "new"
    assert updated_target.audit_trail[1].next_state == "in_review"
    assert updated_target.audit_trail[2].previous_state == "in_review"
    assert updated_target.audit_trail[2].next_state == "resolved"


def test_invalid_transition_raises_value_error() -> None:
    queued = build_extraction_review_queue(_decisions())
    target = [row for row in queued if row.row_id == "row:review"][0]

    with pytest.raises(ValueError, match="invalid state transition"):
        transition_extraction_review_state(
            queued,
            queue_id=target.queue_id,
            next_state="new",
            actor="reviewer-b",
            reason="invalid revert",
        )


def test_deferred_state_allows_return_to_in_review_then_resolved() -> None:
    queued = build_extraction_review_queue(
        _decisions(),
        queued_at=datetime(2026, 1, 2, tzinfo=UTC),
    )
    target = [row for row in queued if row.row_id == "row:review"][0]

    deferred = transition_extraction_review_state(
        queued,
        queue_id=target.queue_id,
        next_state="deferred",
        actor="reviewer-b",
        reason="awaiting additional context",
        changed_at=datetime(2026, 1, 3, tzinfo=UTC),
    )
    resumed = transition_extraction_review_state(
        deferred,
        queue_id=target.queue_id,
        next_state="in_review",
        actor="reviewer-b",
        reason="context received",
        changed_at=datetime(2026, 1, 4, tzinfo=UTC),
    )
    resolved = transition_extraction_review_state(
        resumed,
        queue_id=target.queue_id,
        next_state="resolved",
        actor="reviewer-b",
        reason="resolved after follow-up",
        changed_at=datetime(2026, 1, 5, tzinfo=UTC),
    )
    updated_target = [row for row in resolved if row.queue_id == target.queue_id][0]

    assert updated_target.state == "resolved"
    assert [entry.next_state for entry in updated_target.audit_trail] == [
        "new",
        "deferred",
        "in_review",
        "resolved",
    ]


def test_queue_ingestion_deduplicates_queue_id_preferring_high_priority() -> None:
    queued_at = datetime(2026, 1, 2, tzinfo=UTC)
    duplicate_decisions = [
        ConfidenceRoutingDecision(
            row_id="row:dup",
            plan_id="ca-pers",
            plan_period="FY2025",
            metric_name="discount_rate",
            confidence=0.8,
            routing_outcome="publish_with_warning",
            review_priority="medium",
            publish_blocked=False,
            evidence_refs=("p.10",),
        ),
        ConfidenceRoutingDecision(
            row_id="row:dup",
            plan_id="ca-pers",
            plan_period="FY2025",
            metric_name="discount_rate",
            confidence=0.6,
            routing_outcome="high_priority_review",
            review_priority="high",
            publish_blocked=False,
            evidence_refs=("p.11",),
        ),
    ]

    rows = build_extraction_review_queue(duplicate_decisions, queued_at=queued_at)
    assert len(rows) == 1
    assert rows[0].queue_id == "extraction-review:row:dup"
    assert rows[0].priority == "high"
    assert rows[0].routing_outcome == "high_priority_review"
