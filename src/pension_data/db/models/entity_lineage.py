"""Entity lineage persistence models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

LineageEventType = Literal["rename", "merge", "split", "successor"]


@dataclass(frozen=True, slots=True)
class EntityLineageEvent:
    """One immutable lineage event connecting source and target entities."""

    event_id: str
    event_type: LineageEventType
    source_entity_ids: tuple[str, ...]
    target_entity_ids: tuple[str, ...]
    occurred_at: datetime
    actor: str
    rationale: str
