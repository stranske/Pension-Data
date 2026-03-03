"""Entity registry service input models."""

from __future__ import annotations

from dataclasses import dataclass

from pension_data.db.models.entities import EntityType


@dataclass(frozen=True, slots=True)
class CanonicalEntityDraft:
    """Payload used to create a canonical entity with deterministic stable ID."""

    entity_type: EntityType
    display_name: str
    key_fields: tuple[str, ...] = ()
    metadata: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class SourceRecordProvenance:
    """Source-record provenance payload linked to a canonical entity."""

    source_record_id: str
    source_table: str
    evidence_refs: tuple[str, ...] = ()
