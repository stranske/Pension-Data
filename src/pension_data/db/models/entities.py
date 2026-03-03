"""Canonical entity registry persistence models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

EntityType = Literal["manager", "investment", "vehicle"]


@dataclass(frozen=True, slots=True)
class EntitySourceRecordLink:
    """Provenance link from one canonical entity to one source extraction record."""

    stable_entity_id: str
    source_record_id: str
    source_table: str
    evidence_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CanonicalEntityRecord:
    """Canonical entity row with deterministic stable identity fields."""

    stable_id: str
    entity_type: EntityType
    display_name: str
    normalized_name: str
    normalized_key_fields: tuple[str, ...]
    metadata: tuple[tuple[str, str], ...]
    source_links: tuple[EntitySourceRecordLink, ...]
    merged_into: str | None
    created_at: datetime
    updated_at: datetime
