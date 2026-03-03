"""Review queue persistence models for unresolved entity-candidate decisions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

EntityReviewState = Literal["new", "in_review", "resolved", "deferred"]
EntityDecisionAction = Literal["approve_alias", "merge", "split", "reject"]
EntityType = Literal["manager", "investment", "vehicle"]


@dataclass(frozen=True, slots=True)
class UnresolvedEntityCandidate:
    """Unresolved entity candidate routed to manual review."""

    candidate_id: str
    source_name: str
    entity_type: EntityType
    plan_id: str
    plan_period: str
    candidate_entity_ids: tuple[str, ...]
    provenance_refs: tuple[str, ...]
    confidence: float | None = None


@dataclass(frozen=True, slots=True)
class EntityReviewStateSnapshot:
    """Immutable state snapshot for reproducible entity-review decisions."""

    state: EntityReviewState
    action: EntityDecisionAction | None
    resolved_entity_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class EntityReviewAuditEntry:
    """Immutable audit log entry for an entity-review workflow transition."""

    queue_id: str
    previous_state: EntityReviewState | None
    next_state: EntityReviewState
    action: EntityDecisionAction | None
    reviewer: str
    rationale: str
    changed_at: datetime
    pre_snapshot: EntityReviewStateSnapshot | None
    post_snapshot: EntityReviewStateSnapshot


@dataclass(frozen=True, slots=True)
class EntityReviewQueueRecord:
    """Persisted queue row for unresolved entity-candidate decisions."""

    queue_id: str
    candidate_id: str
    source_name: str
    entity_type: EntityType
    plan_id: str
    plan_period: str
    candidate_entity_ids: tuple[str, ...]
    provenance_refs: tuple[str, ...]
    confidence: float | None
    state: EntityReviewState
    resolution_action: EntityDecisionAction | None
    resolved_entity_ids: tuple[str, ...]
    created_at: datetime
    updated_at: datetime
    audit_trail: tuple[EntityReviewAuditEntry, ...]
