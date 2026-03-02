"""Curated core-fact view builders with strict integrity checks."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from pension_data.db.models.core_facts import (
    ActuarialFact,
    AllocationFact,
    CashFlowFact,
    FeeFact,
    FundedStatusFact,
    HoldingFact,
)


class CuratedIntegrityError(ValueError):
    """Raised when curated views cannot be built from staging rows."""


@dataclass(frozen=True, slots=True)
class CuratedMetricRow:
    """Strict curated metric row with normalized value requirements."""

    plan_id: str
    plan_period: str
    metric_family: str
    metric_name: str
    normalized_value: float
    normalized_unit: str
    effective_date: str
    ingestion_date: str
    benchmark_version: str
    source_document_id: str


@dataclass(frozen=True, slots=True)
class CuratedCashFlowRow:
    """Strict curated cash-flow row with normalized monetary values."""

    plan_id: str
    plan_period: str
    beginning_aum_normalized: float
    ending_aum_normalized: float
    employer_contributions_normalized: float
    employee_contributions_normalized: float
    benefit_payments_normalized: float
    refunds_normalized: float
    effective_date: str
    ingestion_date: str
    benchmark_version: str
    source_document_id: str


def _ensure_plan_is_known(plan_id: str, *, known_plan_ids: set[str]) -> None:
    if plan_id not in known_plan_ids:
        raise CuratedIntegrityError(f"unknown plan_id '{plan_id}' in staging fact")


def _require_normalized_value(value: float | None, *, metric_name: str) -> float:
    if value is None:
        raise CuratedIntegrityError(f"missing normalized value for metric '{metric_name}'")
    return value


def _require_normalized_unit(unit: str | None, *, metric_name: str) -> str:
    if unit is None or not unit.strip():
        raise CuratedIntegrityError(f"missing normalized unit for metric '{metric_name}'")
    return unit.strip()


def _dedupe_metric_rows(rows: Sequence[CuratedMetricRow]) -> list[CuratedMetricRow]:
    deduped: dict[tuple[str, ...], CuratedMetricRow] = {}
    for row in rows:
        key = (
            row.plan_id,
            row.plan_period,
            row.metric_family,
            row.metric_name,
            row.effective_date,
            row.ingestion_date,
            row.benchmark_version,
        )
        if key in deduped:
            raise CuratedIntegrityError(f"duplicate curated metric key detected for {key!r}")
        deduped[key] = row
    return sorted(
        deduped.values(),
        key=lambda row: (
            row.plan_id,
            row.plan_period,
            row.metric_family,
            row.metric_name,
            row.effective_date,
            row.ingestion_date,
        ),
    )


def curated_funded_and_actuarial_rows(
    *,
    funded_facts: Sequence[FundedStatusFact],
    actuarial_facts: Sequence[ActuarialFact],
    known_plan_ids: set[str],
) -> list[CuratedMetricRow]:
    """Build strict curated rows for funded and actuarial fact families."""
    rows: list[CuratedMetricRow] = []
    for row in funded_facts:
        _ensure_plan_is_known(row.context.plan_id, known_plan_ids=known_plan_ids)
        rows.append(
            CuratedMetricRow(
                plan_id=row.context.plan_id,
                plan_period=row.context.plan_period,
                metric_family="funded",
                metric_name=row.metric_name,
                normalized_value=_require_normalized_value(
                    row.metric_value.normalized_value,
                    metric_name=row.metric_name,
                ),
                normalized_unit=_require_normalized_unit(
                    row.metric_value.normalized_unit,
                    metric_name=row.metric_name,
                ),
                effective_date=row.context.effective_date,
                ingestion_date=row.context.ingestion_date,
                benchmark_version=row.context.benchmark_version,
                source_document_id=row.context.source_document_id,
            )
        )
    for actuarial_row in actuarial_facts:
        _ensure_plan_is_known(actuarial_row.context.plan_id, known_plan_ids=known_plan_ids)
        rows.append(
            CuratedMetricRow(
                plan_id=actuarial_row.context.plan_id,
                plan_period=actuarial_row.context.plan_period,
                metric_family="actuarial",
                metric_name=actuarial_row.metric_name,
                normalized_value=_require_normalized_value(
                    actuarial_row.metric_value.normalized_value,
                    metric_name=actuarial_row.metric_name,
                ),
                normalized_unit=_require_normalized_unit(
                    actuarial_row.metric_value.normalized_unit,
                    metric_name=actuarial_row.metric_name,
                ),
                effective_date=actuarial_row.context.effective_date,
                ingestion_date=actuarial_row.context.ingestion_date,
                benchmark_version=actuarial_row.context.benchmark_version,
                source_document_id=actuarial_row.context.source_document_id,
            )
        )
    return _dedupe_metric_rows(rows)


def curated_allocation_rows(
    *,
    allocation_facts: Sequence[AllocationFact],
    known_plan_ids: set[str],
) -> list[CuratedMetricRow]:
    """Build strict curated rows for allocation facts."""
    rows: list[CuratedMetricRow] = []
    for row in allocation_facts:
        _ensure_plan_is_known(row.context.plan_id, known_plan_ids=known_plan_ids)
        rows.append(
            CuratedMetricRow(
                plan_id=row.context.plan_id,
                plan_period=row.context.plan_period,
                metric_family="allocation",
                metric_name=row.metric_name,
                normalized_value=_require_normalized_value(
                    row.metric_value.normalized_value,
                    metric_name=row.metric_name,
                ),
                normalized_unit=_require_normalized_unit(
                    row.metric_value.normalized_unit,
                    metric_name=row.metric_name,
                ),
                effective_date=row.context.effective_date,
                ingestion_date=row.context.ingestion_date,
                benchmark_version=row.context.benchmark_version,
                source_document_id=row.context.source_document_id,
            )
        )
    return _dedupe_metric_rows(rows)


def curated_holding_rows(
    *,
    holding_facts: Sequence[HoldingFact],
    known_plan_ids: set[str],
) -> list[CuratedMetricRow]:
    """Build strict curated rows for holdings facts."""
    rows: list[CuratedMetricRow] = []
    for row in holding_facts:
        _ensure_plan_is_known(row.context.plan_id, known_plan_ids=known_plan_ids)
        if row.relationship_completeness == "complete" and (
            row.manager_name is None or row.fund_name is None
        ):
            raise CuratedIntegrityError(
                "holding row marked complete is missing manager/fund relationship fields"
            )
        rows.append(
            CuratedMetricRow(
                plan_id=row.context.plan_id,
                plan_period=row.context.plan_period,
                metric_family="holding",
                metric_name=row.metric_name,
                normalized_value=_require_normalized_value(
                    row.metric_value.normalized_value,
                    metric_name=row.metric_name,
                ),
                normalized_unit=_require_normalized_unit(
                    row.metric_value.normalized_unit,
                    metric_name=row.metric_name,
                ),
                effective_date=row.context.effective_date,
                ingestion_date=row.context.ingestion_date,
                benchmark_version=row.context.benchmark_version,
                source_document_id=row.context.source_document_id,
            )
        )
    return _dedupe_metric_rows(rows)


def curated_fee_rows(
    *,
    fee_facts: Sequence[FeeFact],
    known_plan_ids: set[str],
) -> list[CuratedMetricRow]:
    """Build strict curated rows for fee facts."""
    rows: list[CuratedMetricRow] = []
    for row in fee_facts:
        _ensure_plan_is_known(row.context.plan_id, known_plan_ids=known_plan_ids)
        rows.append(
            CuratedMetricRow(
                plan_id=row.context.plan_id,
                plan_period=row.context.plan_period,
                metric_family="fee",
                metric_name=row.fee_category,
                normalized_value=_require_normalized_value(
                    row.metric_value.normalized_value,
                    metric_name=row.fee_category,
                ),
                normalized_unit=_require_normalized_unit(
                    row.metric_value.normalized_unit,
                    metric_name=row.fee_category,
                ),
                effective_date=row.context.effective_date,
                ingestion_date=row.context.ingestion_date,
                benchmark_version=row.context.benchmark_version,
                source_document_id=row.context.source_document_id,
            )
        )
    return _dedupe_metric_rows(rows)


def curated_cash_flow_rows(
    *,
    cash_flow_facts: Sequence[CashFlowFact],
    known_plan_ids: set[str],
) -> list[CuratedCashFlowRow]:
    """Build strict curated rows for cash-flow facts."""
    rows: list[CuratedCashFlowRow] = []
    for row in cash_flow_facts:
        _ensure_plan_is_known(row.context.plan_id, known_plan_ids=known_plan_ids)
        rows.append(
            CuratedCashFlowRow(
                plan_id=row.context.plan_id,
                plan_period=row.context.plan_period,
                beginning_aum_normalized=_require_normalized_value(
                    row.beginning_aum.normalized_value,
                    metric_name="beginning_aum",
                ),
                ending_aum_normalized=_require_normalized_value(
                    row.ending_aum.normalized_value,
                    metric_name="ending_aum",
                ),
                employer_contributions_normalized=_require_normalized_value(
                    row.employer_contributions.normalized_value,
                    metric_name="employer_contributions",
                ),
                employee_contributions_normalized=_require_normalized_value(
                    row.employee_contributions.normalized_value,
                    metric_name="employee_contributions",
                ),
                benefit_payments_normalized=_require_normalized_value(
                    row.benefit_payments.normalized_value,
                    metric_name="benefit_payments",
                ),
                refunds_normalized=_require_normalized_value(
                    row.refunds.normalized_value,
                    metric_name="refunds",
                ),
                effective_date=row.context.effective_date,
                ingestion_date=row.context.ingestion_date,
                benchmark_version=row.context.benchmark_version,
                source_document_id=row.context.source_document_id,
            )
        )
    return sorted(
        rows,
        key=lambda row: (
            row.plan_id,
            row.plan_period,
            row.effective_date,
            row.ingestion_date,
            row.benchmark_version,
        ),
    )
