"""Derived entity exposure views for cross-plan manager/fund lookups."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from pension_data.db.models.investment_allocations_fees import AssetAllocationObservation
from pension_data.db.models.investment_positions import PlanManagerFundPosition
from pension_data.db.models.manager_lifecycle import ManagerLifecycleEvent
from pension_data.normalize.entity_tokens import normalize_entity_token


@dataclass(frozen=True, slots=True)
class EntityExposureRow:
    """Cross-plan lookup row with holdings, allocation context, and provenance refs."""

    canonical_entity_id: str
    canonical_entity_type: str
    canonical_entity_name: str
    plan_id: str
    plan_period: str
    manager_name: str | None
    fund_name: str | None
    market_value: float | None
    commitment: float | None
    unfunded: float | None
    exposure_weight: float | None
    allocation_weight_context: float | None
    allocation_amount_usd_context: float | None
    relationship_completeness: str
    known_not_invested: bool
    lifecycle_state: str | None
    evidence_refs: tuple[str, ...]


def _canonical_id(*, entity_type: str, name: str) -> str:
    return f"{entity_type}:{normalize_entity_token(name)}"


def _canonical_fund_id(*, manager_name: str | None, fund_name: str) -> str:
    manager_token = normalize_entity_token(manager_name)
    fund_token = normalize_entity_token(fund_name)
    if manager_token:
        return f"fund:{manager_token}:{fund_token}"
    return f"fund:{fund_token}"


def _dedupe_refs(*values: tuple[str, ...]) -> tuple[str, ...]:
    merged: list[str] = []
    for refs in values:
        for ref in refs:
            token = ref.strip()
            if not token or token in merged:
                continue
            merged.append(token)
    return tuple(merged)


def _allocation_context(
    rows: Sequence[AssetAllocationObservation],
) -> dict[tuple[str, str], tuple[float | None, float | None, tuple[str, ...]]]:
    by_plan_period: dict[tuple[str, str], tuple[float | None, float | None, tuple[str, ...]]] = {}
    grouped: dict[tuple[str, str], list[AssetAllocationObservation]] = {}
    for row in rows:
        grouped.setdefault((row.plan_id, row.plan_period), []).append(row)

    for key, allocation_rows in grouped.items():
        weight_total = sum(
            value
            for value in (item.normalized_weight for item in allocation_rows)
            if value is not None
        )
        amount_total = sum(
            value
            for value in (item.normalized_amount_usd for item in allocation_rows)
            if value is not None
        )
        allocation_refs = _dedupe_refs(*(item.evidence_refs for item in allocation_rows))
        by_plan_period[key] = (
            round(weight_total, 6) if allocation_rows else None,
            round(amount_total, 6) if allocation_rows else None,
            allocation_refs,
        )
    return by_plan_period


def _lifecycle_index(
    events: Sequence[ManagerLifecycleEvent],
) -> dict[tuple[str, str, str, str], tuple[str, tuple[str, ...]]]:
    indexed: dict[tuple[str, str, str, str], tuple[str, tuple[str, ...]]] = {}
    for event in sorted(
        events,
        key=lambda item: (
            item.plan_id,
            item.plan_period,
            normalize_entity_token(item.manager_name),
            normalize_entity_token(item.fund_name),
            item.event_type,
        ),
    ):
        indexed[
            (
                event.plan_id,
                event.plan_period,
                normalize_entity_token(event.manager_name),
                normalize_entity_token(event.fund_name),
            )
        ] = (event.event_type, event.evidence_refs)
    return indexed


def build_entity_exposure_views(
    *,
    positions: Sequence[PlanManagerFundPosition],
    allocation_observations: Sequence[AssetAllocationObservation] = (),
    lifecycle_events: Sequence[ManagerLifecycleEvent] = (),
) -> list[EntityExposureRow]:
    """Build deterministic cross-plan entity exposure rows with evidence context."""
    sorted_positions = sorted(
        positions,
        key=lambda row: (
            row.plan_id,
            row.plan_period,
            normalize_entity_token(row.manager_name),
            normalize_entity_token(row.fund_name),
        ),
    )
    market_total_by_plan_period: dict[tuple[str, str], float] = {}
    for row in sorted_positions:
        if row.market_value is None:
            continue
        key = (row.plan_id, row.plan_period)
        market_total_by_plan_period[key] = (
            market_total_by_plan_period.get(key, 0.0) + row.market_value
        )

    allocation_context = _allocation_context(allocation_observations)
    lifecycle_state_by_key = _lifecycle_index(lifecycle_events)
    exposure_rows: list[EntityExposureRow] = []

    for position in sorted_positions:
        plan_period_key = (position.plan_id, position.plan_period)
        market_total = market_total_by_plan_period.get(plan_period_key, 0.0)
        exposure_weight = (
            round(position.market_value / market_total, 9)
            if position.market_value is not None and market_total > 0
            else None
        )
        allocation_weight, allocation_amount_usd, allocation_refs = allocation_context.get(
            plan_period_key, (None, None, ())
        )
        lifecycle_state, lifecycle_refs = lifecycle_state_by_key.get(
            (
                position.plan_id,
                position.plan_period,
                normalize_entity_token(position.manager_name),
                normalize_entity_token(position.fund_name),
            ),
            (None, ()),
        )
        evidence_refs = _dedupe_refs(position.evidence_refs, allocation_refs, lifecycle_refs)

        if position.manager_name and position.manager_name.strip():
            exposure_rows.append(
                EntityExposureRow(
                    canonical_entity_id=_canonical_id(
                        entity_type="manager", name=position.manager_name
                    ),
                    canonical_entity_type="manager",
                    canonical_entity_name=position.manager_name,
                    plan_id=position.plan_id,
                    plan_period=position.plan_period,
                    manager_name=position.manager_name,
                    fund_name=position.fund_name,
                    market_value=position.market_value,
                    commitment=position.commitment,
                    unfunded=position.unfunded,
                    exposure_weight=exposure_weight,
                    allocation_weight_context=allocation_weight,
                    allocation_amount_usd_context=allocation_amount_usd,
                    relationship_completeness=position.completeness,
                    known_not_invested=position.known_not_invested,
                    lifecycle_state=lifecycle_state,
                    evidence_refs=evidence_refs,
                )
            )

        if position.fund_name and position.fund_name.strip():
            exposure_rows.append(
                EntityExposureRow(
                    canonical_entity_id=_canonical_fund_id(
                        manager_name=position.manager_name,
                        fund_name=position.fund_name,
                    ),
                    canonical_entity_type="fund",
                    canonical_entity_name=position.fund_name,
                    plan_id=position.plan_id,
                    plan_period=position.plan_period,
                    manager_name=position.manager_name,
                    fund_name=position.fund_name,
                    market_value=position.market_value,
                    commitment=position.commitment,
                    unfunded=position.unfunded,
                    exposure_weight=exposure_weight,
                    allocation_weight_context=allocation_weight,
                    allocation_amount_usd_context=allocation_amount_usd,
                    relationship_completeness=position.completeness,
                    known_not_invested=position.known_not_invested,
                    lifecycle_state=lifecycle_state,
                    evidence_refs=evidence_refs,
                )
            )

    return sorted(
        exposure_rows,
        key=lambda row: (
            row.canonical_entity_id,
            row.plan_id,
            row.plan_period,
            normalize_entity_token(row.manager_name),
            normalize_entity_token(row.fund_name),
        ),
    )
