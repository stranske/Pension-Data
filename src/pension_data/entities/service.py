"""Canonical entity registry services."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime

from pension_data.db.models.entities import (
    CanonicalEntityRecord,
    EntitySourceRecordLink,
    EntityType,
)
from pension_data.entities.models import CanonicalEntityDraft, SourceRecordProvenance
from pension_data.normalize.entity_tokens import normalize_entity_token


def _normalize_utc(dt: datetime | None) -> datetime:
    current = dt or datetime.now(UTC)
    return current.replace(tzinfo=UTC) if current.tzinfo is None else current.astimezone(UTC)


def _normalize_tokens(values: Sequence[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for value in values:
        token = normalize_entity_token(value)
        if not token:
            continue
        normalized.append(token)
    return tuple(normalized)


def _normalize_metadata(values: Sequence[tuple[str, str]]) -> tuple[tuple[str, str], ...]:
    normalized: dict[str, str] = {}
    for key, value in values:
        normalized_key = normalize_entity_token(key)
        if not normalized_key:
            continue
        normalized[normalized_key] = value.strip()
    return tuple(sorted(normalized.items(), key=lambda item: item[0]))


def build_canonical_stable_id(
    *,
    entity_type: EntityType,
    display_name: str,
    key_fields: Sequence[str] = (),
) -> str:
    """Build deterministic stable ID for one canonical entity payload."""
    normalized_name = normalize_entity_token(display_name)
    if not normalized_name:
        raise ValueError("display_name must normalize to a non-empty entity token")
    normalized_keys = _normalize_tokens(key_fields)
    if not normalized_keys:
        return f"{entity_type}:{normalized_name}"
    return f"{entity_type}:{normalized_name}:{':'.join(normalized_keys)}"


def _sorted_entities(rows: Sequence[CanonicalEntityRecord]) -> list[CanonicalEntityRecord]:
    return sorted(rows, key=lambda row: row.stable_id)


def create_canonical_entity(
    rows: Sequence[CanonicalEntityRecord],
    *,
    draft: CanonicalEntityDraft,
    created_at: datetime | None = None,
) -> list[CanonicalEntityRecord]:
    """Create one canonical entity and enforce deterministic uniqueness."""
    stable_id = build_canonical_stable_id(
        entity_type=draft.entity_type,
        display_name=draft.display_name,
        key_fields=draft.key_fields,
    )
    if any(row.stable_id == stable_id and row.merged_into is None for row in rows):
        raise ValueError(f"duplicate canonical entity for stable_id '{stable_id}'")

    timestamp = _normalize_utc(created_at)
    normalized_name = normalize_entity_token(draft.display_name)
    normalized_key_fields = _normalize_tokens(draft.key_fields)
    entity = CanonicalEntityRecord(
        stable_id=stable_id,
        entity_type=draft.entity_type,
        display_name=draft.display_name.strip(),
        normalized_name=normalized_name,
        normalized_key_fields=normalized_key_fields,
        metadata=_normalize_metadata(draft.metadata),
        source_links=(),
        merged_into=None,
        created_at=timestamp,
        updated_at=timestamp,
    )
    return _sorted_entities([*rows, entity])


def update_canonical_entity_metadata(
    rows: Sequence[CanonicalEntityRecord],
    *,
    stable_id: str,
    metadata: Sequence[tuple[str, str]],
    updated_at: datetime | None = None,
) -> list[CanonicalEntityRecord]:
    """Update entity metadata without changing stable identity fields."""
    timestamp = _normalize_utc(updated_at)
    updated_rows: list[CanonicalEntityRecord] = []
    found = False
    for row in rows:
        if row.stable_id != stable_id:
            updated_rows.append(row)
            continue
        found = True
        if row.merged_into is not None:
            raise ValueError(f"cannot update merged entity '{stable_id}'")
        updated_rows.append(
            replace(
                row,
                metadata=_normalize_metadata(metadata),
                updated_at=timestamp,
            )
        )
    if not found:
        raise ValueError(f"canonical entity not found for stable_id '{stable_id}'")
    return _sorted_entities(updated_rows)


def link_source_record(
    rows: Sequence[CanonicalEntityRecord],
    *,
    stable_id: str,
    provenance: SourceRecordProvenance,
    linked_at: datetime | None = None,
) -> list[CanonicalEntityRecord]:
    """Attach one provenance record link to a canonical entity stable ID."""
    timestamp = _normalize_utc(linked_at)
    updated_rows: list[CanonicalEntityRecord] = []
    found = False

    for row in rows:
        if row.stable_id != stable_id:
            updated_rows.append(row)
            continue
        found = True
        if row.merged_into is not None:
            raise ValueError(f"cannot link provenance to merged entity '{stable_id}'")

        link_key = (provenance.source_table.strip(), provenance.source_record_id.strip())
        if not link_key[0] or not link_key[1]:
            raise ValueError("source_table and source_record_id are required")

        existing_links = list(row.source_links)
        evidence_refs = tuple(
            value
            for value in dict.fromkeys(
                ref.strip() for ref in provenance.evidence_refs if ref.strip()
            )
        )
        merged = False
        for index, item in enumerate(existing_links):
            if (item.source_table, item.source_record_id) != link_key:
                continue
            merged_refs = tuple(
                value for value in dict.fromkeys([*item.evidence_refs, *evidence_refs]) if value
            )
            existing_links[index] = replace(item, evidence_refs=merged_refs)
            merged = True
            break

        if not merged:
            existing_links.append(
                EntitySourceRecordLink(
                    stable_entity_id=row.stable_id,
                    source_record_id=link_key[1],
                    source_table=link_key[0],
                    evidence_refs=evidence_refs,
                )
            )
        updated_rows.append(
            replace(
                row,
                source_links=tuple(
                    sorted(
                        existing_links,
                        key=lambda item: (item.source_table, item.source_record_id),
                    )
                ),
                updated_at=timestamp,
            )
        )

    if not found:
        raise ValueError(f"canonical entity not found for stable_id '{stable_id}'")
    return _sorted_entities(updated_rows)


def merge_canonical_entities(
    rows: Sequence[CanonicalEntityRecord],
    *,
    source_stable_id: str,
    target_stable_id: str,
    merged_at: datetime | None = None,
) -> list[CanonicalEntityRecord]:
    """Merge one canonical entity into another using an explicit merge path."""
    if source_stable_id == target_stable_id:
        raise ValueError("source and target stable IDs must differ for merge")

    timestamp = _normalize_utc(merged_at)
    by_id = {row.stable_id: row for row in rows}
    source = by_id.get(source_stable_id)
    target = by_id.get(target_stable_id)
    if source is None:
        raise ValueError(f"source canonical entity not found for '{source_stable_id}'")
    if target is None:
        raise ValueError(f"target canonical entity not found for '{target_stable_id}'")
    if source.merged_into is not None:
        raise ValueError(f"source canonical entity '{source_stable_id}' is already merged")
    if target.merged_into is not None:
        raise ValueError(f"target canonical entity '{target_stable_id}' is merged and unavailable")

    updated = [
        (
            replace(source, merged_into=target_stable_id, updated_at=timestamp)
            if row.stable_id == source_stable_id
            else row
        )
        for row in rows
    ]
    return _sorted_entities(updated)


def list_active_canonical_entities(
    rows: Sequence[CanonicalEntityRecord],
) -> list[CanonicalEntityRecord]:
    """Return active canonical entities in deterministic stable-id ordering."""
    return _sorted_entities([row for row in rows if row.merged_into is None])
