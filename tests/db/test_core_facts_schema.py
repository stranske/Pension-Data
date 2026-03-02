"""Tests for bitemporal dual-reporting core fact schema and curated views."""

from __future__ import annotations

from pathlib import Path

import pytest

from pension_data.db.models.core_facts import (
    ActuarialFact,
    AllocationFact,
    BitemporalFactContext,
    CashFlowFact,
    DualReportedValue,
    FeeFact,
    FundedStatusFact,
    HoldingFact,
    query_bitemporal_as_of,
)
from pension_data.db.views.core_facts import (
    CuratedIntegrityError,
    curated_allocation_rows,
    curated_cash_flow_rows,
    curated_fee_rows,
    curated_funded_and_actuarial_rows,
    curated_holding_rows,
)

ROOT = Path(__file__).resolve().parents[2]


def _context(
    *,
    effective_date: str,
    ingestion_date: str,
    plan_id: str = "CA-PERS",
    plan_period: str = "FY2024",
) -> BitemporalFactContext:
    return BitemporalFactContext(
        plan_id=plan_id,
        plan_period=plan_period,
        effective_date=effective_date,
        ingestion_date=ingestion_date,
        benchmark_version="v1",
        source_document_id="doc:ca:2024",
    )


def _value(*, as_reported: float | None, normalized: float | None, unit: str) -> DualReportedValue:
    return DualReportedValue(
        as_reported_value=as_reported,
        normalized_value=normalized,
        as_reported_unit=unit,
        normalized_unit=unit,
    )


def test_dual_value_structure_persists_as_reported_and_normalized_values() -> None:
    fact = FundedStatusFact(
        context=_context(
            effective_date="2024-06-30",
            ingestion_date="2025-01-15",
        ),
        metric_name="funded_ratio",
        metric_value=_value(as_reported=0.781, normalized=0.781, unit="ratio"),
        confidence=0.95,
        evidence_refs=("p.45",),
    )
    assert fact.metric_value.as_reported_value == 0.781
    assert fact.metric_value.normalized_value == 0.781
    assert fact.context.effective_date == "2024-06-30"
    assert fact.context.ingestion_date == "2025-01-15"


def test_query_bitemporal_as_of_filters_by_effective_and_ingestion_dates() -> None:
    facts = [
        FundedStatusFact(
            context=_context(
                effective_date="2024-06-30",
                ingestion_date="2025-01-01",
            ),
            metric_name="funded_ratio",
            metric_value=_value(as_reported=0.77, normalized=0.77, unit="ratio"),
            confidence=0.9,
            evidence_refs=("p.1",),
        ),
        FundedStatusFact(
            context=_context(
                effective_date="2025-06-30",
                ingestion_date="2025-08-01",
            ),
            metric_name="funded_ratio",
            metric_value=_value(as_reported=0.79, normalized=0.79, unit="ratio"),
            confidence=0.9,
            evidence_refs=("p.2",),
        ),
    ]
    as_of = query_bitemporal_as_of(
        facts,
        effective_date="2024-12-31",
        ingestion_date="2025-03-01",
    )
    assert len(as_of) == 1
    assert as_of[0].metric_value.normalized_value == 0.77


def test_curated_view_builders_enforce_known_plan_and_normalized_values() -> None:
    known_plan_ids = {"CA-PERS"}
    funded = [
        FundedStatusFact(
            context=_context(
                effective_date="2024-06-30",
                ingestion_date="2025-01-01",
                plan_id="TX-ERS",
            ),
            metric_name="funded_ratio",
            metric_value=_value(as_reported=0.75, normalized=0.75, unit="ratio"),
            confidence=0.9,
            evidence_refs=("p.8",),
        )
    ]
    with pytest.raises(CuratedIntegrityError, match="unknown plan_id"):
        curated_funded_and_actuarial_rows(
            funded_facts=funded,
            actuarial_facts=[],
            known_plan_ids=known_plan_ids,
        )

    funded_missing_normalized = [
        FundedStatusFact(
            context=_context(
                effective_date="2024-06-30",
                ingestion_date="2025-01-01",
            ),
            metric_name="funded_ratio",
            metric_value=_value(as_reported=0.75, normalized=None, unit="ratio"),
            confidence=0.9,
            evidence_refs=("p.8",),
        )
    ]
    with pytest.raises(CuratedIntegrityError, match="missing normalized value"):
        curated_funded_and_actuarial_rows(
            funded_facts=funded_missing_normalized,
            actuarial_facts=[],
            known_plan_ids=known_plan_ids,
        )


def test_curated_rows_build_successfully_for_all_metric_families() -> None:
    known_plan_ids = {"CA-PERS"}
    funded_rows = curated_funded_and_actuarial_rows(
        funded_facts=[
            FundedStatusFact(
                context=_context(
                    effective_date="2024-06-30",
                    ingestion_date="2025-01-01",
                ),
                metric_name="funded_ratio",
                metric_value=_value(as_reported=0.8, normalized=0.8, unit="ratio"),
                confidence=0.9,
                evidence_refs=("p.10",),
            )
        ],
        actuarial_facts=[
            ActuarialFact(
                context=_context(
                    effective_date="2024-06-30",
                    ingestion_date="2025-01-01",
                ),
                metric_name="normal_cost_rate",
                metric_value=_value(as_reported=0.14, normalized=0.14, unit="ratio"),
                confidence=0.88,
                evidence_refs=("p.11",),
            )
        ],
        known_plan_ids=known_plan_ids,
    )
    assert len(funded_rows) == 2
    assert funded_rows[0].metric_family in {"actuarial", "funded"}

    allocation_rows = curated_allocation_rows(
        allocation_facts=[
            AllocationFact(
                context=_context(
                    effective_date="2024-06-30",
                    ingestion_date="2025-01-01",
                ),
                metric_name="public_equity_weight",
                metric_value=_value(as_reported=0.45, normalized=0.45, unit="ratio"),
                confidence=0.92,
                evidence_refs=("p.12",),
            )
        ],
        known_plan_ids=known_plan_ids,
    )
    assert allocation_rows[0].metric_family == "allocation"

    holding_rows = curated_holding_rows(
        holding_facts=[
            HoldingFact(
                context=_context(
                    effective_date="2024-06-30",
                    ingestion_date="2025-01-01",
                ),
                manager_name="Manager A",
                fund_name="Fund A",
                vehicle_name="Vehicle A",
                metric_name="market_value",
                metric_value=_value(as_reported=125_000_000, normalized=125_000_000, unit="usd"),
                relationship_completeness="complete",
                confidence=0.9,
                evidence_refs=("p.13",),
            )
        ],
        known_plan_ids=known_plan_ids,
    )
    assert holding_rows[0].metric_family == "holding"

    fee_rows = curated_fee_rows(
        fee_facts=[
            FeeFact(
                context=_context(
                    effective_date="2024-06-30",
                    ingestion_date="2025-01-01",
                ),
                fee_category="investment_management",
                manager_name="Manager A",
                metric_value=_value(as_reported=0.004, normalized=0.004, unit="ratio"),
                confidence=0.87,
                evidence_refs=("p.14",),
            )
        ],
        known_plan_ids=known_plan_ids,
    )
    assert fee_rows[0].metric_family == "fee"

    cash_rows = curated_cash_flow_rows(
        cash_flow_facts=[
            CashFlowFact(
                context=_context(
                    effective_date="2024-06-30",
                    ingestion_date="2025-01-01",
                ),
                beginning_aum=_value(as_reported=1_000, normalized=1_000, unit="usd"),
                ending_aum=_value(as_reported=1_100, normalized=1_100, unit="usd"),
                employer_contributions=_value(as_reported=80, normalized=80, unit="usd"),
                employee_contributions=_value(as_reported=30, normalized=30, unit="usd"),
                benefit_payments=_value(as_reported=60, normalized=60, unit="usd"),
                refunds=_value(as_reported=5, normalized=5, unit="usd"),
                confidence=0.9,
                evidence_refs=("p.15",),
            )
        ],
        known_plan_ids=known_plan_ids,
    )
    assert cash_rows[0].beginning_aum_normalized == 1_000


def test_migration_files_include_required_bitemporal_columns_and_cash_flow_fields() -> None:
    migration_path = (
        ROOT / "src" / "pension_data" / "db" / "migrations" / "20260302_001_core_fact_staging.sql"
    )
    assert migration_path.exists()
    sql = migration_path.read_text(encoding="utf-8")
    for column in (
        "plan_period",
        "benchmark_version",
        "effective_date",
        "ingestion_date",
        "beginning_aum",
        "ending_aum",
        "employer_contributions",
        "employee_contributions",
        "benefit_payments",
        "refunds",
    ):
        assert column in sql
