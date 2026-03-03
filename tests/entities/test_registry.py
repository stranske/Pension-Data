"""Tests for canonical entity registry models and services."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pension_data.entities.models import CanonicalEntityDraft, SourceRecordProvenance
from pension_data.entities.service import (
    build_canonical_stable_id,
    create_canonical_entity,
    link_source_record,
    list_active_canonical_entities,
    merge_canonical_entities,
    update_canonical_entity_metadata,
)


def test_create_entity_persists_stable_id_and_metadata() -> None:
    rows = create_canonical_entity(
        [],
        draft=CanonicalEntityDraft(
            entity_type="manager",
            display_name="Alpha Capital",
            key_fields=("US", "Private Equity"),
            metadata=(("Jurisdiction", "US"), ("Strategy", "Private Equity")),
        ),
        created_at=datetime(2026, 1, 5, tzinfo=UTC),
    )

    assert len(rows) == 1
    assert rows[0].stable_id == "manager:alpha capital:us:private equity"
    assert rows[0].normalized_name == "alpha capital"
    assert rows[0].normalized_key_fields == ("us", "private equity")
    assert rows[0].metadata == (("jurisdiction", "US"), ("strategy", "Private Equity"))


def test_duplicate_canonical_entity_is_blocked_by_deterministic_id() -> None:
    rows = create_canonical_entity(
        [],
        draft=CanonicalEntityDraft(entity_type="manager", display_name="Alpha Capital"),
    )

    with pytest.raises(ValueError, match="duplicate canonical entity"):
        create_canonical_entity(
            rows,
            draft=CanonicalEntityDraft(entity_type="manager", display_name="alpha   capital"),
        )


def test_metadata_update_preserves_stable_id() -> None:
    rows = create_canonical_entity(
        [],
        draft=CanonicalEntityDraft(entity_type="investment", display_name="Fund I"),
    )
    stable_id = rows[0].stable_id
    updated = update_canonical_entity_metadata(
        rows,
        stable_id=stable_id,
        metadata=(("asset_class", "private equity"),),
        updated_at=datetime(2026, 1, 6, tzinfo=UTC),
    )

    assert updated[0].stable_id == stable_id
    assert updated[0].metadata == (("asset class", "private equity"),)
    assert updated[0].updated_at == datetime(2026, 1, 6, tzinfo=UTC)


def test_source_record_links_attach_via_stable_foreign_key_and_dedupe() -> None:
    rows = create_canonical_entity(
        [],
        draft=CanonicalEntityDraft(entity_type="manager", display_name="Beta Partners"),
    )
    stable_id = rows[0].stable_id
    linked = link_source_record(
        rows,
        stable_id=stable_id,
        provenance=SourceRecordProvenance(
            source_record_id="extraction:row:1",
            source_table="investment_positions",
            evidence_refs=("Page 12",),
        ),
    )
    linked_again = link_source_record(
        linked,
        stable_id=stable_id,
        provenance=SourceRecordProvenance(
            source_record_id="extraction:row:1",
            source_table="investment_positions",
            evidence_refs=("p.13", "p.12"),
        ),
    )

    assert len(linked_again[0].source_links) == 1
    link = linked_again[0].source_links[0]
    assert link.stable_entity_id == stable_id
    assert link.source_table == "investment_positions"
    assert link.source_record_id == "extraction:row:1"
    assert link.evidence_refs == ("p.12", "p.13")


def test_explicit_merge_path_marks_source_and_filters_active_entities() -> None:
    rows = create_canonical_entity(
        [],
        draft=CanonicalEntityDraft(entity_type="manager", display_name="Alpha Capital"),
    )
    rows = create_canonical_entity(
        rows,
        draft=CanonicalEntityDraft(
            entity_type="manager",
            display_name="Alpha Capital LP",
        ),
    )
    source_stable_id = "manager:alpha capital lp"
    target_stable_id = "manager:alpha capital"
    merged = merge_canonical_entities(
        rows,
        source_stable_id=source_stable_id,
        target_stable_id=target_stable_id,
        merged_at=datetime(2026, 1, 7, tzinfo=UTC),
    )
    active = list_active_canonical_entities(merged)
    source_row = [row for row in merged if row.stable_id == source_stable_id][0]

    assert source_row.merged_into == target_stable_id
    assert [row.stable_id for row in active] == [target_stable_id]
    with pytest.raises(ValueError, match="source and target stable IDs must differ"):
        merge_canonical_entities(
            merged,
            source_stable_id=target_stable_id,
            target_stable_id=target_stable_id,
        )


def test_merge_rejects_cross_type_entities() -> None:
    rows = create_canonical_entity(
        [],
        draft=CanonicalEntityDraft(entity_type="manager", display_name="Alpha Capital"),
    )
    rows = create_canonical_entity(
        rows,
        draft=CanonicalEntityDraft(entity_type="vehicle", display_name="Alpha Capital Fund I"),
    )

    with pytest.raises(ValueError, match="entity_type must match"):
        merge_canonical_entities(
            rows,
            source_stable_id="vehicle:alpha capital fund i",
            target_stable_id="manager:alpha capital",
        )


def test_merge_transfers_source_links_to_target_with_deduped_evidence() -> None:
    rows = create_canonical_entity(
        [],
        draft=CanonicalEntityDraft(entity_type="manager", display_name="Alpha Capital"),
    )
    rows = create_canonical_entity(
        rows,
        draft=CanonicalEntityDraft(entity_type="manager", display_name="Alpha Capital LP"),
    )
    rows = link_source_record(
        rows,
        stable_id="manager:alpha capital",
        provenance=SourceRecordProvenance(
            source_record_id="fact:1",
            source_table="holdings",
            evidence_refs=("p.10",),
        ),
    )
    rows = link_source_record(
        rows,
        stable_id="manager:alpha capital lp",
        provenance=SourceRecordProvenance(
            source_record_id="fact:1",
            source_table="holdings",
            evidence_refs=("p.11", "page 10"),
        ),
    )

    merged = merge_canonical_entities(
        rows,
        source_stable_id="manager:alpha capital lp",
        target_stable_id="manager:alpha capital",
    )
    by_id = {row.stable_id: row for row in merged}
    target = by_id["manager:alpha capital"]
    source = by_id["manager:alpha capital lp"]

    assert source.merged_into == "manager:alpha capital"
    assert source.source_links == ()
    assert len(target.source_links) == 1
    assert target.source_links[0].stable_entity_id == "manager:alpha capital"
    assert target.source_links[0].evidence_refs == ("p.10", "p.11")


def test_create_rejects_reuse_of_stable_id_after_merge() -> None:
    rows = create_canonical_entity(
        [],
        draft=CanonicalEntityDraft(entity_type="manager", display_name="Delta Capital"),
    )
    rows = create_canonical_entity(
        rows,
        draft=CanonicalEntityDraft(entity_type="manager", display_name="Delta Capital II"),
    )
    merged = merge_canonical_entities(
        rows,
        source_stable_id="manager:delta capital ii",
        target_stable_id="manager:delta capital",
    )

    with pytest.raises(ValueError, match="duplicate canonical entity"):
        create_canonical_entity(
            merged,
            draft=CanonicalEntityDraft(entity_type="manager", display_name="Delta Capital II"),
        )


def test_build_canonical_stable_id_is_deterministic() -> None:
    stable_id = build_canonical_stable_id(
        entity_type="vehicle",
        display_name=" Vehicle III ",
        key_fields=("2025", "Series A"),
    )

    assert stable_id == "vehicle:vehicle iii:2025:series a"
