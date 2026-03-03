"""Review queue ingestion and decision workflow for unresolved entity candidates."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime

from pension_data.db.models.review_queue_entities import (
    EntityDecisionAction,
    EntityReviewAuditEntry,
    EntityReviewQueueRecord,
    EntityReviewState,
    EntityReviewStateSnapshot,
    UnresolvedEntityCandidate,
)

_ALLOWED_TRANSITIONS: dict[EntityReviewState, set[EntityReviewState]] = {
    "new": {"in_review", "resolved", "deferred"},
    "in_review": {"resolved", "deferred"},
    "deferred": {"in_review", "resolved"},
    "resolved": set(),
}

_STATE_ORDER: dict[EntityReviewState, int] = {
    "new": 0,
    "in_review": 1,
    "deferred": 2,
    "resolved": 3,
}

_VALID_DECISIONS: set[EntityDecisionAction] = {
    "approve_alias",
    "merge",
    "split",
    "reject",
}


def _normalize_utc(dt: datetime | None) -> datetime:
    current = dt or datetime.now(UTC)
    return current.replace(tzinfo=UTC) if current.tzinfo is None else current.astimezone(UTC)


def _confidence_rank(value: float | None) -> tuple[int, float]:
    if value is None:
        return (0, -1.0)
    return (1, value)


def _snapshot(
    *,
    state: EntityReviewState,
    action: EntityDecisionAction | None,
    resolved_entity_ids: tuple[str, ...],
) -> EntityReviewStateSnapshot:
    return EntityReviewStateSnapshot(
        state=state,
        action=action,
        resolved_entity_ids=resolved_entity_ids,
    )


def _sort_key(row: EntityReviewQueueRecord) -> tuple[int, str, str, str, str]:
    return (
        _STATE_ORDER[row.state],
        row.plan_id,
        row.plan_period,
        row.source_name.casefold(),
        row.candidate_id,
    )


def ingest_entity_review_candidates(
    candidates: Sequence[UnresolvedEntityCandidate],
    *,
    queued_at: datetime | None = None,
) -> list[EntityReviewQueueRecord]:
    """Persist unresolved entity candidates into deterministic review queue rows."""
    created_at = _normalize_utc(queued_at)
    queue_rows_by_id: dict[str, EntityReviewQueueRecord] = {}

    for candidate in sorted(
        candidates,
        key=lambda row: (
            row.plan_id,
            row.plan_period,
            row.source_name.casefold(),
            row.candidate_id,
        ),
    ):
        queue_id = f"entity-review:{candidate.candidate_id}"
        post_snapshot = _snapshot(
            state="new",
            action=None,
            resolved_entity_ids=(),
        )
        queue_row = EntityReviewQueueRecord(
            queue_id=queue_id,
            candidate_id=candidate.candidate_id,
            source_name=candidate.source_name,
            entity_type=candidate.entity_type,
            plan_id=candidate.plan_id,
            plan_period=candidate.plan_period,
            candidate_entity_ids=candidate.candidate_entity_ids,
            provenance_refs=candidate.provenance_refs,
            confidence=candidate.confidence,
            state="new",
            resolution_action=None,
            resolved_entity_ids=(),
            created_at=created_at,
            updated_at=created_at,
            audit_trail=(
                EntityReviewAuditEntry(
                    queue_id=queue_id,
                    previous_state=None,
                    next_state="new",
                    action=None,
                    reviewer="system",
                    rationale="ingested:unresolved_entity_candidate",
                    changed_at=created_at,
                    pre_snapshot=None,
                    post_snapshot=post_snapshot,
                ),
            ),
        )
        existing = queue_rows_by_id.get(queue_id)
        if existing is None or _confidence_rank(queue_row.confidence) > _confidence_rank(
            existing.confidence
        ):
            queue_rows_by_id[queue_id] = queue_row

    return sorted(queue_rows_by_id.values(), key=_sort_key)


def _normalize_resolved_ids(values: Sequence[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for value in values:
        token = value.strip()
        if not token or token in normalized:
            continue
        normalized.append(token)
    return tuple(normalized)


def _validate_resolved_action(
    *,
    next_state: EntityReviewState,
    action: EntityDecisionAction | None,
    resolved_entity_ids: tuple[str, ...],
) -> None:
    if next_state != "resolved":
        if action is not None:
            raise ValueError("non-resolved transition cannot set resolution action")
        if resolved_entity_ids:
            raise ValueError("non-resolved transition cannot set resolved entities")
        return

    if action is None:
        raise ValueError("resolved transition requires a decision action")
    if action not in _VALID_DECISIONS:
        raise ValueError(f"unsupported decision action '{action}'")

    if action in {"approve_alias", "merge"} and not resolved_entity_ids:
        raise ValueError(f"{action} decision requires at least one resolved entity id")
    if action == "split" and len(resolved_entity_ids) < 2:
        raise ValueError("split decision requires at least two resolved entity ids")
    if action == "reject" and resolved_entity_ids:
        raise ValueError("reject decision cannot include resolved entity ids")


def apply_entity_review_decision(
    queue_rows: Sequence[EntityReviewQueueRecord],
    *,
    queue_id: str,
    next_state: EntityReviewState,
    reviewer: str,
    rationale: str,
    action: EntityDecisionAction | None = None,
    resolved_entity_ids: Sequence[str] = (),
    changed_at: datetime | None = None,
) -> list[EntityReviewQueueRecord]:
    """Apply one reviewer workflow transition with immutable audit snapshots."""
    timestamp = _normalize_utc(changed_at)
    normalized_resolved_ids = _normalize_resolved_ids(resolved_entity_ids)
    updated_rows: list[EntityReviewQueueRecord] = []
    found = False

    for row in queue_rows:
        if row.queue_id != queue_id:
            updated_rows.append(row)
            continue
        found = True
        if next_state not in _ALLOWED_TRANSITIONS[row.state]:
            raise ValueError(f"invalid state transition: {row.state} -> {next_state}")
        _validate_resolved_action(
            next_state=next_state,
            action=action,
            resolved_entity_ids=normalized_resolved_ids,
        )

        next_action = action if next_state == "resolved" else None
        next_resolved_ids = normalized_resolved_ids if next_state == "resolved" else ()
        pre_snapshot = _snapshot(
            state=row.state,
            action=row.resolution_action,
            resolved_entity_ids=row.resolved_entity_ids,
        )
        post_snapshot = _snapshot(
            state=next_state,
            action=next_action,
            resolved_entity_ids=next_resolved_ids,
        )
        audit_entry = EntityReviewAuditEntry(
            queue_id=row.queue_id,
            previous_state=row.state,
            next_state=next_state,
            action=next_action,
            reviewer=reviewer.strip() or "unknown",
            rationale=rationale.strip() or "state transition",
            changed_at=timestamp,
            pre_snapshot=pre_snapshot,
            post_snapshot=post_snapshot,
        )
        updated_rows.append(
            replace(
                row,
                state=next_state,
                resolution_action=next_action,
                resolved_entity_ids=next_resolved_ids,
                updated_at=timestamp,
                audit_trail=(*row.audit_trail, audit_entry),
            )
        )

    if not found:
        raise ValueError(f"queue row not found for queue_id '{queue_id}'")

    return sorted(updated_rows, key=_sort_key)


def list_pending_entity_reviews(
    queue_rows: Sequence[EntityReviewQueueRecord],
) -> list[EntityReviewQueueRecord]:
    """Return deterministic pending review rows (new/in_review/deferred)."""
    return sorted([row for row in queue_rows if row.state != "resolved"], key=_sort_key)


def list_resolved_entity_reviews(
    queue_rows: Sequence[EntityReviewQueueRecord],
) -> list[EntityReviewQueueRecord]:
    """Return deterministic resolved review rows."""
    return sorted([row for row in queue_rows if row.state == "resolved"], key=_sort_key)
