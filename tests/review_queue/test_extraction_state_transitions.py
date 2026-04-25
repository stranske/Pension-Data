"""Regression tests for review queue state machine and routing consistency."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from pension_data.quality.confidence import ConfidenceRoutingDecision
from pension_data.review_queue.extraction import (
    build_extraction_review_queue,
    transition_extraction_review_state,
)

if TYPE_CHECKING:
    from pension_data.db.models.review_queue import ExtractionReviewQueueRecord


def _decision(
    *,
    row_id: str = "row-1",
    plan_id: str = "CA-PERS",
    plan_period: str = "FY2024",
    metric_name: str = "funded_ratio",
    confidence: float = 0.80,
    routing_outcome: str = "publish_with_warning",
    review_priority: str = "medium",
) -> ConfidenceRoutingDecision:
    return ConfidenceRoutingDecision(
        row_id=row_id,
        plan_id=plan_id,
        plan_period=plan_period,
        metric_name=metric_name,
        confidence=confidence,
        routing_outcome=routing_outcome,
        review_priority=review_priority,
        publish_blocked=False,
        evidence_refs=("p.1",),
    )


# ── build_extraction_review_queue ───────────────────────────────────


class TestBuildExtractionReviewQueue:
    def test_warning_routes_to_queue(self) -> None:
        decisions = [_decision()]
        rows = build_extraction_review_queue(decisions)
        assert len(rows) == 1
        assert rows[0].state == "new"
        assert rows[0].priority == "medium"

    def test_high_priority_routes_to_queue(self) -> None:
        decisions = [
            _decision(
                confidence=0.50,
                routing_outcome="high_priority_review",
                review_priority="high",
            )
        ]
        rows = build_extraction_review_queue(decisions)
        assert len(rows) == 1
        assert rows[0].priority == "high"

    def test_auto_accept_skipped(self) -> None:
        decisions = [
            _decision(
                confidence=0.95,
                routing_outcome="auto_accept",
                review_priority="none",
            )
        ]
        rows = build_extraction_review_queue(decisions)
        assert len(rows) == 0

    def test_inconsistent_routing_raises(self) -> None:
        with pytest.raises(ValueError, match="inconsistent"):
            build_extraction_review_queue([
                _decision(
                    routing_outcome="publish_with_warning",
                    review_priority="high",
                )
            ])

    def test_inconsistent_high_priority_raises(self) -> None:
        with pytest.raises(ValueError, match="inconsistent"):
            build_extraction_review_queue([
                _decision(
                    routing_outcome="high_priority_review",
                    review_priority="medium",
                )
            ])

    def test_audit_trail_populated(self) -> None:
        rows = build_extraction_review_queue([_decision()])
        assert len(rows[0].audit_trail) == 1
        entry = rows[0].audit_trail[0]
        assert entry.previous_state is None
        assert entry.next_state == "new"
        assert entry.actor == "system"

    def test_sorted_high_before_medium(self) -> None:
        decisions = [
            _decision(
                row_id="row-1",
                routing_outcome="publish_with_warning",
                review_priority="medium",
                confidence=0.80,
            ),
            _decision(
                row_id="row-2",
                routing_outcome="high_priority_review",
                review_priority="high",
                confidence=0.50,
            ),
        ]
        rows = build_extraction_review_queue(decisions)
        assert len(rows) == 2
        assert rows[0].priority == "high"
        assert rows[1].priority == "medium"

    def test_duplicate_row_id_keeps_higher_priority(self) -> None:
        decisions = [
            _decision(
                row_id="row-1",
                routing_outcome="publish_with_warning",
                review_priority="medium",
                confidence=0.80,
            ),
            _decision(
                row_id="row-1",
                routing_outcome="high_priority_review",
                review_priority="high",
                confidence=0.50,
            ),
        ]
        rows = build_extraction_review_queue(decisions)
        assert len(rows) == 1
        assert rows[0].priority == "high"


# ── transition_extraction_review_state ──────────────────────────────


class TestTransitionState:
    @pytest.fixture()
    def _base_rows(self) -> list[ExtractionReviewQueueRecord]:
        ts = datetime(2024, 1, 1, tzinfo=UTC)
        return build_extraction_review_queue(
            [_decision()], queued_at=ts
        )

    def test_new_to_in_review(self, _base_rows: list[ExtractionReviewQueueRecord]) -> None:
        result = transition_extraction_review_state(
            _base_rows,
            queue_id=_base_rows[0].queue_id,
            next_state="in_review",
            actor="reviewer",
            reason="starting review",
        )
        assert result[0].state == "in_review"

    def test_new_to_resolved(self, _base_rows: list[ExtractionReviewQueueRecord]) -> None:
        result = transition_extraction_review_state(
            _base_rows,
            queue_id=_base_rows[0].queue_id,
            next_state="resolved",
            actor="reviewer",
            reason="approved",
        )
        assert result[0].state == "resolved"

    def test_new_to_deferred(self, _base_rows: list[ExtractionReviewQueueRecord]) -> None:
        result = transition_extraction_review_state(
            _base_rows,
            queue_id=_base_rows[0].queue_id,
            next_state="deferred",
            actor="reviewer",
            reason="needs more info",
        )
        assert result[0].state == "deferred"

    def test_resolved_to_anything_raises(
        self, _base_rows: list[ExtractionReviewQueueRecord]
    ) -> None:
        resolved = transition_extraction_review_state(
            _base_rows,
            queue_id=_base_rows[0].queue_id,
            next_state="resolved",
            actor="reviewer",
            reason="done",
        )
        with pytest.raises(ValueError, match="invalid state transition"):
            transition_extraction_review_state(
                resolved,
                queue_id=resolved[0].queue_id,
                next_state="in_review",
                actor="reviewer",
                reason="reopen",
            )

    def test_deferred_to_in_review(
        self, _base_rows: list[ExtractionReviewQueueRecord]
    ) -> None:
        deferred = transition_extraction_review_state(
            _base_rows,
            queue_id=_base_rows[0].queue_id,
            next_state="deferred",
            actor="reviewer",
            reason="needs more info",
        )
        result = transition_extraction_review_state(
            deferred,
            queue_id=deferred[0].queue_id,
            next_state="in_review",
            actor="reviewer",
            reason="resuming review",
        )
        assert result[0].state == "in_review"

    def test_audit_trail_grows(
        self, _base_rows: list[ExtractionReviewQueueRecord]
    ) -> None:
        result = transition_extraction_review_state(
            _base_rows,
            queue_id=_base_rows[0].queue_id,
            next_state="in_review",
            actor="reviewer",
            reason="starting review",
        )
        # Initial (system) + this transition
        assert len(result[0].audit_trail) == 2
        latest = result[0].audit_trail[-1]
        assert latest.previous_state == "new"
        assert latest.next_state == "in_review"
        assert latest.actor == "reviewer"

    def test_unknown_queue_id_raises(
        self, _base_rows: list[ExtractionReviewQueueRecord]
    ) -> None:
        with pytest.raises(ValueError, match="not found"):
            transition_extraction_review_state(
                _base_rows,
                queue_id="nonexistent",
                next_state="in_review",
                actor="reviewer",
                reason="test",
            )

    def test_empty_actor_defaults_to_unknown(
        self, _base_rows: list[ExtractionReviewQueueRecord]
    ) -> None:
        result = transition_extraction_review_state(
            _base_rows,
            queue_id=_base_rows[0].queue_id,
            next_state="in_review",
            actor="",
            reason="test",
        )
        assert result[0].audit_trail[-1].actor == "unknown"

    def test_empty_reason_defaults(
        self, _base_rows: list[ExtractionReviewQueueRecord]
    ) -> None:
        result = transition_extraction_review_state(
            _base_rows,
            queue_id=_base_rows[0].queue_id,
            next_state="in_review",
            actor="reviewer",
            reason="",
        )
        assert result[0].audit_trail[-1].reason == "state transition"
