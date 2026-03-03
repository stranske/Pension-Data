"""Entity lineage event operations and traversal utilities."""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Sequence
from datetime import UTC, datetime

from pension_data.db.models.entity_lineage import EntityLineageEvent, LineageEventType


def _normalize_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def _normalize_entity_ids(values: Sequence[str]) -> tuple[str, ...]:
    normalized = [value.strip() for value in values if value and value.strip()]
    deduped = tuple(dict.fromkeys(normalized))
    if not deduped:
        raise ValueError("lineage event must include at least one non-empty entity id")
    return deduped


def _validate_event_shape(
    *,
    event_type: LineageEventType,
    source_entity_ids: tuple[str, ...],
    target_entity_ids: tuple[str, ...],
) -> None:
    if set(source_entity_ids).intersection(target_entity_ids):
        raise ValueError("lineage source and target entity ids must not overlap")

    if event_type == "rename":
        if len(source_entity_ids) != 1 or len(target_entity_ids) != 1:
            raise ValueError("rename requires exactly one source and one target entity id")
        return
    if event_type == "merge":
        if len(source_entity_ids) < 2 or len(target_entity_ids) != 1:
            raise ValueError("merge requires at least two sources and exactly one target")
        return
    if event_type == "split":
        if len(source_entity_ids) != 1 or len(target_entity_ids) < 2:
            raise ValueError("split requires exactly one source and at least two targets")
        return
    if event_type == "successor":
        if len(source_entity_ids) != 1 or len(target_entity_ids) != 1:
            raise ValueError("successor requires exactly one source and one target")
        return
    raise ValueError(f"unsupported lineage event_type '{event_type}'")


def _sorted_events(events: Sequence[EntityLineageEvent]) -> list[EntityLineageEvent]:
    return sorted(events, key=lambda event: (event.occurred_at, event.event_id))


def _build_forward_graph(events: Sequence[EntityLineageEvent]) -> dict[str, set[str]]:
    graph: dict[str, set[str]] = defaultdict(set)
    for event in _sorted_events(events):
        for source in event.source_entity_ids:
            graph[source].update(event.target_entity_ids)
    return graph


def _assert_no_cycles(events: Sequence[EntityLineageEvent]) -> None:
    graph = _build_forward_graph(events)
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str) -> None:
        if node in visiting:
            raise ValueError(f"circular lineage detected at entity '{node}'")
        if node in visited:
            return
        visiting.add(node)
        for child in sorted(graph.get(node, set())):
            visit(child)
        visiting.remove(node)
        visited.add(node)

    for node in sorted(graph):
        visit(node)


def record_lineage_event(
    events: Sequence[EntityLineageEvent],
    *,
    event_id: str,
    event_type: LineageEventType,
    source_entity_ids: Sequence[str],
    target_entity_ids: Sequence[str],
    occurred_at: datetime,
    actor: str,
    rationale: str,
) -> list[EntityLineageEvent]:
    """Persist one lineage event with schema constraints and cycle validation."""
    normalized_event_id = event_id.strip()
    if not normalized_event_id:
        raise ValueError("event_id is required")
    if any(item.event_id == normalized_event_id for item in events):
        raise ValueError(f"duplicate lineage event_id '{normalized_event_id}'")

    normalized_sources = _normalize_entity_ids(source_entity_ids)
    normalized_targets = _normalize_entity_ids(target_entity_ids)
    _validate_event_shape(
        event_type=event_type,
        source_entity_ids=normalized_sources,
        target_entity_ids=normalized_targets,
    )

    event = EntityLineageEvent(
        event_id=normalized_event_id,
        event_type=event_type,
        source_entity_ids=normalized_sources,
        target_entity_ids=normalized_targets,
        occurred_at=_normalize_utc(occurred_at),
        actor=actor.strip() or "unknown",
        rationale=rationale.strip() or "lineage update",
    )
    candidate = [*events, event]
    _assert_no_cycles(candidate)
    return _sorted_events(candidate)


def successor_chain(
    events: Sequence[EntityLineageEvent],
    *,
    entity_id: str,
) -> list[str]:
    """Return deterministic breadth-first successor chain for one entity ID."""
    graph = _build_forward_graph(events)
    start = entity_id.strip()
    if not start:
        return []

    queue: deque[str] = deque(sorted(graph.get(start, set())))
    seen: set[str] = set()
    ordered: list[str] = []
    while queue:
        current = queue.popleft()
        if current in seen:
            continue
        seen.add(current)
        ordered.append(current)
        for child in sorted(graph.get(current, set())):
            if child not in seen:
                queue.append(child)
    return ordered


def historical_predecessors(
    events: Sequence[EntityLineageEvent],
    *,
    entity_id: str,
) -> list[str]:
    """Return deterministic predecessor list for one current/historical entity ID."""
    reverse_graph: dict[str, set[str]] = defaultdict(set)
    for event in _sorted_events(events):
        for target in event.target_entity_ids:
            reverse_graph[target].update(event.source_entity_ids)

    start = entity_id.strip()
    if not start:
        return []

    queue: deque[str] = deque(sorted(reverse_graph.get(start, set())))
    seen: set[str] = set()
    ordered: list[str] = []
    while queue:
        current = queue.popleft()
        if current in seen:
            continue
        seen.add(current)
        ordered.append(current)
        for predecessor in sorted(reverse_graph.get(current, set())):
            if predecessor not in seen:
                queue.append(predecessor)
    return ordered


def current_canonical_entity_id(
    events: Sequence[EntityLineageEvent],
    *,
    entity_id: str,
) -> str:
    """Resolve one entity ID to a current canonical ID through successor traversal."""
    current = entity_id.strip()
    if not current:
        raise ValueError("entity_id is required")

    graph = _build_forward_graph(events)
    visited: set[str] = set()
    while True:
        if current in visited:
            raise ValueError(f"circular lineage detected at entity '{current}'")
        visited.add(current)
        successors = sorted(graph.get(current, set()))
        if not successors:
            return current
        if len(successors) > 1:
            raise ValueError(
                f"entity '{current}' has multiple successors; canonical resolution is ambiguous"
            )
        current = successors[0]
