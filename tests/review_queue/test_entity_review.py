"""Tests for entity review queue ingestion and reviewer decision workflow."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pension_data.db.models.review_queue_entities import UnresolvedEntityCandidate
from pension_data.review_queue.entities import (
    apply_entity_review_decision,
    ingest_entity_review_candidates,
    list_pending_entity_reviews,
    list_resolved_entity_reviews,
)


def _candidates() -> list[UnresolvedEntityCandidate]:
    return [
        UnresolvedEntityCandidate(
            candidate_id="cand:alpha",
            source_name="Alpha Capital",
            entity_type="manager",
            plan_id="CA-PERS",
            plan_period="FY2025",
            candidate_entity_ids=("manager:alpha capital", "manager:alpha cap"),
            provenance_refs=("doc:ca:2025:p45",),
            confidence=0.78,
        ),
        UnresolvedEntityCandidate(
            candidate_id="cand:beta",
            source_name="Beta Partners",
            entity_type="manager",
            plan_id="TX-ERS",
            plan_period="FY2025",
            candidate_entity_ids=("manager:beta partners",),
            provenance_refs=("doc:tx:2025:p18",),
            confidence=0.91,
        ),
    ]


def test_ingestion_persists_provenance_and_creates_new_state_audit_entry() -> None:
    rows = ingest_entity_review_candidates(
        _candidates(),
        queued_at=datetime(2026, 1, 10, tzinfo=UTC),
    )

    assert [row.queue_id for row in rows] == [
        "entity-review:cand:alpha",
        "entity-review:cand:beta",
    ]
    assert rows[0].provenance_refs == ("doc:ca:2025:p45",)
    assert all(row.state == "new" for row in rows)
    assert all(len(row.audit_trail) == 1 for row in rows)
    assert all(row.audit_trail[0].previous_state is None for row in rows)
    assert all(row.audit_trail[0].next_state == "new" for row in rows)
    assert all(row.audit_trail[0].reviewer == "system" for row in rows)


def test_reviewer_decision_actions_are_auditable_with_pre_post_snapshots() -> None:
    queued = ingest_entity_review_candidates(
        _candidates(),
        queued_at=datetime(2026, 1, 10, tzinfo=UTC),
    )

    in_review = apply_entity_review_decision(
        queued,
        queue_id="entity-review:cand:alpha",
        next_state="in_review",
        reviewer="reviewer-a",
        rationale="triage started",
        changed_at=datetime(2026, 1, 11, tzinfo=UTC),
    )
    resolved = apply_entity_review_decision(
        in_review,
        queue_id="entity-review:cand:alpha",
        next_state="resolved",
        reviewer="reviewer-a",
        rationale="matched canonical manager record",
        action="approve_alias",
        resolved_entity_ids=("manager:alpha capital",),
        changed_at=datetime(2026, 1, 12, tzinfo=UTC),
    )
    row = [item for item in resolved if item.queue_id == "entity-review:cand:alpha"][0]

    assert row.state == "resolved"
    assert row.resolution_action == "approve_alias"
    assert row.resolved_entity_ids == ("manager:alpha capital",)
    assert len(row.audit_trail) == 3
    assert row.audit_trail[1].pre_snapshot is not None
    assert row.audit_trail[1].pre_snapshot.state == "new"
    assert row.audit_trail[1].post_snapshot.state == "in_review"
    assert row.audit_trail[2].action == "approve_alias"
    assert row.audit_trail[2].pre_snapshot is not None
    assert row.audit_trail[2].pre_snapshot.state == "in_review"
    assert row.audit_trail[2].post_snapshot.state == "resolved"
    assert row.audit_trail[2].post_snapshot.resolved_entity_ids == ("manager:alpha capital",)


def test_invalid_transition_is_rejected() -> None:
    queued = ingest_entity_review_candidates(_candidates())

    with pytest.raises(ValueError, match="invalid state transition"):
        apply_entity_review_decision(
            queued,
            queue_id="entity-review:cand:alpha",
            next_state="new",
            reviewer="reviewer-b",
            rationale="invalid revert",
        )


def test_invalid_resolved_decision_payload_is_rejected() -> None:
    queued = ingest_entity_review_candidates(_candidates())

    with pytest.raises(ValueError, match="split decision requires at least two"):
        apply_entity_review_decision(
            queued,
            queue_id="entity-review:cand:alpha",
            next_state="resolved",
            reviewer="reviewer-b",
            rationale="split test",
            action="split",
            resolved_entity_ids=("manager:alpha capital",),
        )

    with pytest.raises(ValueError, match="reject decision cannot include"):
        apply_entity_review_decision(
            queued,
            queue_id="entity-review:cand:alpha",
            next_state="resolved",
            reviewer="reviewer-b",
            rationale="reject test",
            action="reject",
            resolved_entity_ids=("manager:alpha capital",),
        )


def test_pending_and_resolved_lists_are_deterministic() -> None:
    queued = ingest_entity_review_candidates(_candidates())
    deferred = apply_entity_review_decision(
        queued,
        queue_id="entity-review:cand:beta",
        next_state="deferred",
        reviewer="reviewer-c",
        rationale="need additional context",
    )
    resolved = apply_entity_review_decision(
        deferred,
        queue_id="entity-review:cand:alpha",
        next_state="resolved",
        reviewer="reviewer-c",
        rationale="canonical mapping confirmed",
        action="merge",
        resolved_entity_ids=("manager:alpha capital",),
    )

    pending = list_pending_entity_reviews(resolved)
    final = list_resolved_entity_reviews(resolved)

    assert [row.queue_id for row in pending] == ["entity-review:cand:beta"]
    assert [row.state for row in pending] == ["deferred"]
    assert [row.queue_id for row in final] == ["entity-review:cand:alpha"]
    assert [row.state for row in final] == ["resolved"]
