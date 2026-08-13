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


@dataclass(frozen=True, slots=True)
class SponsorPlanKey:
    """Canonical, non-PII join key for a Form 5500 sponsor and plan."""

    ein: str
    plan_number: str

    def __post_init__(self) -> None:
        normalized_ein = "".join(character for character in self.ein if character.isdigit())
        normalized_plan = "".join(character for character in self.plan_number if character.isdigit())
        if len(normalized_ein) != 9:
            raise ValueError("EIN must contain exactly nine digits")
        if not normalized_plan or len(normalized_plan) > 3:
            raise ValueError("plan number must contain one to three digits")
        object.__setattr__(self, "ein", normalized_ein)
        object.__setattr__(self, "plan_number", normalized_plan.zfill(3))
