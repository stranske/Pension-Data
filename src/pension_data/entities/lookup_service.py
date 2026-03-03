"""Indexed cross-plan entity exposure lookup service."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass

from pension_data.db.views.entity_exposure_views import EntityExposureRow
from pension_data.normalize.entity_tokens import normalize_entity_token


@dataclass(frozen=True, slots=True)
class EntityExposureIndex:
    """Indexed lookup structures for entity exposure retrieval patterns."""

    by_entity_id: dict[str, tuple[EntityExposureRow, ...]]
    by_plan_period: dict[tuple[str, str], tuple[EntityExposureRow, ...]]
    by_entity_plan_period: dict[tuple[str, str, str], tuple[EntityExposureRow, ...]]
    alias_to_entity_id: dict[str, str]
    total_rows: int


@dataclass(frozen=True, slots=True)
class LookupExecutionTrace:
    """Execution metadata for lookup diagnostics and performance assertions."""

    used_index: bool
    total_rows: int
    candidate_count: int
    resolved_entity_id: str | None


def build_entity_exposure_index(rows: Sequence[EntityExposureRow]) -> EntityExposureIndex:
    """Build deterministic indexes for frequent entity lookup access paths."""
    grouped_by_entity: dict[str, list[EntityExposureRow]] = defaultdict(list)
    grouped_by_plan_period: dict[tuple[str, str], list[EntityExposureRow]] = defaultdict(list)
    grouped_by_entity_plan_period: dict[tuple[str, str, str], list[EntityExposureRow]] = (
        defaultdict(list)
    )
    alias_candidates: dict[str, set[str]] = defaultdict(set)

    for row in sorted(
        rows,
        key=lambda item: (
            item.canonical_entity_id,
            item.plan_id,
            item.plan_period,
            item.manager_name or "",
            item.fund_name or "",
        ),
    ):
        grouped_by_entity[row.canonical_entity_id].append(row)
        grouped_by_plan_period[(row.plan_id, row.plan_period)].append(row)
        grouped_by_entity_plan_period[
            (row.canonical_entity_id, row.plan_id, row.plan_period)
        ].append(row)
        alias_candidates[normalize_entity_token(row.canonical_entity_name)].add(
            row.canonical_entity_id
        )
        if row.canonical_entity_type == "manager" and row.manager_name:
            alias_candidates[normalize_entity_token(row.manager_name)].add(row.canonical_entity_id)
        if row.canonical_entity_type == "fund" and row.fund_name:
            alias_candidates[normalize_entity_token(row.fund_name)].add(row.canonical_entity_id)

    alias_to_entity_id = {
        alias: next(iter(entity_ids))
        for alias, entity_ids in alias_candidates.items()
        if len(entity_ids) == 1
    }
    return EntityExposureIndex(
        by_entity_id={
            key: tuple(value)
            for key, value in sorted(grouped_by_entity.items(), key=lambda item: item[0])
        },
        by_plan_period={
            key: tuple(value)
            for key, value in sorted(grouped_by_plan_period.items(), key=lambda item: item[0])
        },
        by_entity_plan_period={
            key: tuple(value)
            for key, value in sorted(
                grouped_by_entity_plan_period.items(), key=lambda item: item[0]
            )
        },
        alias_to_entity_id=dict(sorted(alias_to_entity_id.items(), key=lambda item: item[0])),
        total_rows=len(rows),
    )


def _normalize_canonical_query(entity_query: str) -> str | None:
    entity_type, sep, remainder = entity_query.partition(":")
    if not sep:
        return None

    normalized_type = normalize_entity_token(entity_type)
    if normalized_type not in {"manager", "fund"}:
        return None

    if normalized_type == "manager":
        normalized_name = normalize_entity_token(remainder)
        return f"manager:{normalized_name}" if normalized_name else None

    manager_part, has_fund_sep, fund_part = remainder.partition(":")
    if has_fund_sep:
        normalized_manager = normalize_entity_token(manager_part)
        normalized_fund = normalize_entity_token(fund_part)
        if normalized_manager and normalized_fund:
            return f"fund:{normalized_manager}:{normalized_fund}"
        return None

    normalized_fund = normalize_entity_token(remainder)
    return f"fund:{normalized_fund}" if normalized_fund else None


def resolve_canonical_entity_id(
    index: EntityExposureIndex,
    *,
    entity_query: str,
) -> str | None:
    """Resolve an entity query against canonical IDs and unique alias mapping."""
    if entity_query in index.by_entity_id:
        return entity_query
    normalized_canonical_query = _normalize_canonical_query(entity_query)
    if normalized_canonical_query and normalized_canonical_query in index.by_entity_id:
        return normalized_canonical_query
    return index.alias_to_entity_id.get(normalize_entity_token(entity_query))


def lookup_entity_exposures(
    index: EntityExposureIndex,
    *,
    entity_query: str,
    plan_id: str | None = None,
    plan_period: str | None = None,
) -> tuple[list[EntityExposureRow], LookupExecutionTrace]:
    """Lookup exposures by canonical entity id or alias with index-backed filtering."""
    resolved_entity_id = resolve_canonical_entity_id(index, entity_query=entity_query)
    if resolved_entity_id is None:
        return (
            [],
            LookupExecutionTrace(
                used_index=True,
                total_rows=index.total_rows,
                candidate_count=0,
                resolved_entity_id=None,
            ),
        )

    if plan_id is not None and plan_period is not None:
        candidates = list(
            index.by_entity_plan_period.get((resolved_entity_id, plan_id, plan_period), ())
        )
    else:
        candidates = list(index.by_entity_id.get(resolved_entity_id, ()))
        if plan_id is not None:
            candidates = [row for row in candidates if row.plan_id == plan_id]
        if plan_period is not None:
            candidates = [row for row in candidates if row.plan_period == plan_period]

    ordered = sorted(
        candidates,
        key=lambda row: (
            row.canonical_entity_id,
            row.plan_id,
            row.plan_period,
            row.manager_name or "",
            row.fund_name or "",
        ),
    )
    return (
        ordered,
        LookupExecutionTrace(
            used_index=True,
            total_rows=index.total_rows,
            candidate_count=len(candidates),
            resolved_entity_id=resolved_entity_id,
        ),
    )
