"""Tests for cross-plan entity lookup views and index-backed service."""

from __future__ import annotations

from pension_data.db.models.investment_allocations_fees import AssetAllocationObservation
from pension_data.db.models.investment_positions import PlanManagerFundPosition
from pension_data.db.models.manager_lifecycle import ManagerLifecycleEvent
from pension_data.db.views.entity_exposure_views import build_entity_exposure_views
from pension_data.entities.lookup_service import (
    build_entity_exposure_index,
    lookup_entity_exposures,
)


def _positions() -> list[PlanManagerFundPosition]:
    return [
        PlanManagerFundPosition(
            plan_id="CA-PERS",
            plan_period="FY2025",
            manager_name="Alpha Capital",
            fund_name="Fund I",
            commitment=120.0,
            unfunded=24.0,
            market_value=96.0,
            completeness="complete",
            confidence=0.94,
            evidence_refs=("p.45",),
        ),
        PlanManagerFundPosition(
            plan_id="TX-ERS",
            plan_period="FY2025",
            manager_name="alpha   capital",
            fund_name="Fund I",
            commitment=80.0,
            unfunded=18.0,
            market_value=62.0,
            completeness="complete",
            confidence=0.90,
            evidence_refs=("p.18",),
        ),
        PlanManagerFundPosition(
            plan_id="TX-ERS",
            plan_period="FY2025",
            manager_name="Beta Partners",
            fund_name=None,
            commitment=None,
            unfunded=None,
            market_value=None,
            completeness="not_disclosed",
            known_not_invested=False,
            confidence=0.80,
            evidence_refs=("p.20",),
            warnings=("non_disclosure",),
        ),
    ]


def _allocations() -> list[AssetAllocationObservation]:
    return [
        AssetAllocationObservation(
            plan_id="CA-PERS",
            plan_period="FY2025",
            category="public_equity",
            as_reported_percent=42.0,
            normalized_weight=0.42,
            as_reported_amount=420.0,
            normalized_amount_usd=420_000_000.0,
            effective_date="2025-06-30",
            ingestion_date="2026-01-15",
            source_document_id="doc:ca:2025:allocations",
            evidence_refs=("p.30",),
        ),
        AssetAllocationObservation(
            plan_id="TX-ERS",
            plan_period="FY2025",
            category="public_equity",
            as_reported_percent=38.0,
            normalized_weight=0.38,
            as_reported_amount=180.0,
            normalized_amount_usd=180_000_000.0,
            effective_date="2025-08-31",
            ingestion_date="2026-02-02",
            source_document_id="doc:tx:2025:allocations",
            evidence_refs=("p.12",),
        ),
    ]


def _lifecycle_events() -> list[ManagerLifecycleEvent]:
    return [
        ManagerLifecycleEvent(
            plan_id="CA-PERS",
            plan_period="FY2025",
            manager_name="Alpha Capital",
            fund_name="Fund I",
            event_type="still_invested",
            basis="inferred",
            confidence=0.92,
            evidence_refs=("p.46",),
        )
    ]


def test_cross_plan_lookup_returns_exposures_with_holdings_allocation_and_evidence_context() -> (
    None
):
    rows = build_entity_exposure_views(
        positions=_positions(),
        allocation_observations=_allocations(),
        lifecycle_events=_lifecycle_events(),
    )
    index = build_entity_exposure_index(rows)
    results, trace = lookup_entity_exposures(index, entity_query="manager:alpha capital")

    assert trace.used_index is True
    assert trace.candidate_count == 2
    assert {row.plan_id for row in results} == {"CA-PERS", "TX-ERS"}
    assert all(row.market_value is not None for row in results)
    assert all(row.exposure_weight is not None for row in results)
    assert all(row.allocation_weight_context is not None for row in results)
    assert all(row.allocation_amount_usd_context is not None for row in results)
    assert any("p.45" in row.evidence_refs for row in results)
    assert any(row.lifecycle_state == "still_invested" for row in results)


def test_lookup_resolves_aliases_and_includes_non_disclosure_states() -> None:
    rows = build_entity_exposure_views(
        positions=_positions(),
        allocation_observations=_allocations(),
        lifecycle_events=_lifecycle_events(),
    )
    index = build_entity_exposure_index(rows)
    alias_results, alias_trace = lookup_entity_exposures(
        index,
        entity_query="ALPHA CAPITAL",
    )
    nondisclosure_results, _ = lookup_entity_exposures(
        index,
        entity_query="Beta Partners",
    )

    assert alias_trace.resolved_entity_id == "manager:alpha capital"
    assert len(alias_results) == 2
    assert len(nondisclosure_results) == 1
    assert nondisclosure_results[0].relationship_completeness == "not_disclosed"
    assert nondisclosure_results[0].market_value is None


def test_lookup_index_limits_candidate_scan_for_performance_sensitive_queries() -> None:
    expanded_positions = list(_positions())
    for index in range(300):
        expanded_positions.append(
            PlanManagerFundPosition(
                plan_id=f"PLAN-{index:03d}",
                plan_period="FY2025",
                manager_name=f"Manager {index:03d}",
                fund_name=f"Fund {index:03d}",
                commitment=10.0,
                unfunded=2.0,
                market_value=8.0,
                completeness="complete",
                confidence=0.8,
                evidence_refs=("p.1",),
            )
        )

    materialized_rows = build_entity_exposure_views(positions=expanded_positions)
    index = build_entity_exposure_index(materialized_rows)
    results, trace = lookup_entity_exposures(index, entity_query="manager:alpha capital")

    assert len(materialized_rows) > 300
    assert len(results) == 2
    assert trace.candidate_count == 2
    assert trace.total_rows == len(materialized_rows)
