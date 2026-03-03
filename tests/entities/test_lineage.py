"""Tests for entity lineage event model and traversal utilities."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pension_data.entities.lineage import (
    current_canonical_entity_id,
    historical_predecessors,
    record_lineage_event,
    successor_chain,
)


def test_rename_and_successor_events_resolve_current_and_historical_paths() -> None:
    events = record_lineage_event(
        [],
        event_id="evt:rename:1",
        event_type="rename",
        source_entity_ids=("manager:alpha old",),
        target_entity_ids=("manager:alpha",),
        occurred_at=datetime(2025, 1, 1, tzinfo=UTC),
        actor="reviewer-a",
        rationale="legal rename",
    )
    events = record_lineage_event(
        events,
        event_id="evt:successor:1",
        event_type="successor",
        source_entity_ids=("manager:alpha",),
        target_entity_ids=("manager:alpha holdings",),
        occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
        actor="reviewer-a",
        rationale="successor vehicle",
    )

    assert successor_chain(events, entity_id="manager:alpha old") == [
        "manager:alpha",
        "manager:alpha holdings",
    ]
    assert historical_predecessors(events, entity_id="manager:alpha holdings") == [
        "manager:alpha",
        "manager:alpha old",
    ]
    assert (
        current_canonical_entity_id(events, entity_id="manager:alpha old")
        == "manager:alpha holdings"
    )


def test_merge_event_resolves_multiple_predecessors_to_single_current() -> None:
    events = record_lineage_event(
        [],
        event_id="evt:merge:1",
        event_type="merge",
        source_entity_ids=("manager:beta i", "manager:beta ii"),
        target_entity_ids=("manager:beta merged",),
        occurred_at=datetime(2026, 2, 1, tzinfo=UTC),
        actor="reviewer-b",
        rationale="merge approved",
    )

    assert current_canonical_entity_id(events, entity_id="manager:beta i") == "manager:beta merged"
    assert current_canonical_entity_id(events, entity_id="manager:beta ii") == "manager:beta merged"
    assert historical_predecessors(events, entity_id="manager:beta merged") == [
        "manager:beta i",
        "manager:beta ii",
    ]


def test_split_event_reports_ambiguous_current_canonical_resolution() -> None:
    events = record_lineage_event(
        [],
        event_id="evt:split:1",
        event_type="split",
        source_entity_ids=("manager:gamma",),
        target_entity_ids=("manager:gamma a", "manager:gamma b"),
        occurred_at=datetime(2026, 3, 1, tzinfo=UTC),
        actor="reviewer-c",
        rationale="organizational split",
    )

    assert successor_chain(events, entity_id="manager:gamma") == [
        "manager:gamma a",
        "manager:gamma b",
    ]
    with pytest.raises(ValueError, match="multiple successors"):
        current_canonical_entity_id(events, entity_id="manager:gamma")


def test_invalid_event_shapes_are_rejected() -> None:
    with pytest.raises(ValueError, match="rename requires exactly one source and one target"):
        record_lineage_event(
            [],
            event_id="evt:bad-rename",
            event_type="rename",
            source_entity_ids=("a", "b"),
            target_entity_ids=("c",),
            occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
            actor="reviewer-d",
            rationale="invalid rename shape",
        )

    with pytest.raises(ValueError, match="merge requires at least two sources"):
        record_lineage_event(
            [],
            event_id="evt:bad-merge",
            event_type="merge",
            source_entity_ids=("a",),
            target_entity_ids=("b",),
            occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
            actor="reviewer-d",
            rationale="invalid merge shape",
        )

    with pytest.raises(ValueError, match="split requires exactly one source"):
        record_lineage_event(
            [],
            event_id="evt:bad-split",
            event_type="split",
            source_entity_ids=("a", "b"),
            target_entity_ids=("c", "d"),
            occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
            actor="reviewer-d",
            rationale="invalid split shape",
        )


def test_circular_lineage_is_rejected_explicitly() -> None:
    events = record_lineage_event(
        [],
        event_id="evt:successor:a",
        event_type="successor",
        source_entity_ids=("manager:delta",),
        target_entity_ids=("manager:delta next",),
        occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
        actor="reviewer-e",
        rationale="successor established",
    )

    with pytest.raises(ValueError, match="circular lineage detected"):
        record_lineage_event(
            events,
            event_id="evt:successor:b",
            event_type="successor",
            source_entity_ids=("manager:delta next",),
            target_entity_ids=("manager:delta",),
            occurred_at=datetime(2026, 1, 2, tzinfo=UTC),
            actor="reviewer-e",
            rationale="invalid cycle",
        )
