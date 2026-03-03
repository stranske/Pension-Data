"""Bitemporal core fact staging models with dual-reporting support."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Protocol, TypeVar

RelationshipCompleteness = Literal["complete", "partial", "not_disclosed"]
FeeCategory = Literal["investment_management", "performance", "consulting", "other"]

FundedMetricName = Literal[
    "funded_ratio",
    "actuarial_accrued_liability",
    "actuarial_value_of_assets",
    "discount_rate",
    "employer_contribution_rate",
    "employee_contribution_rate",
    "participant_count",
]
ActuarialMetricName = Literal[
    "normal_cost_rate",
    "amortization_payment",
    "payroll_growth_assumption",
]
AllocationMetricName = Literal[
    "public_equity_weight",
    "fixed_income_weight",
    "private_equity_weight",
    "real_assets_weight",
    "cash_weight",
]
HoldingMetricName = Literal["market_value", "commitment", "unfunded", "weight_pct"]


@dataclass(frozen=True, slots=True)
class DualReportedValue:
    """Dual-reporting value container for as-reported and normalized metrics."""

    as_reported_value: float | None
    normalized_value: float | None
    as_reported_unit: str | None
    normalized_unit: str | None


@dataclass(frozen=True, slots=True)
class BitemporalFactContext:
    """Common bitemporal metadata for staging facts."""

    plan_id: str
    plan_period: str
    effective_date: str
    ingestion_date: str
    benchmark_version: str
    source_document_id: str


class _HasBitemporalContext(Protocol):
    context: BitemporalFactContext


TFact = TypeVar("TFact", bound=_HasBitemporalContext)


def _parse_iso_temporal(value: str, *, field_name: str) -> datetime:
    candidate = value.strip()
    if not candidate:
        raise ValueError(f"{field_name} must be a non-empty ISO-8601 date or datetime string")
    normalized = f"{candidate[:-1]}+00:00" if candidate.endswith("Z") else candidate
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:  # pragma: no cover - defensive parse guard
        raise ValueError(
            f"{field_name} must be an ISO-8601 date or datetime string: {value!r}"
        ) from exc
    if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class FundedStatusFact:
    """Staging fact row for funded-status metrics."""

    context: BitemporalFactContext
    metric_name: FundedMetricName
    metric_value: DualReportedValue
    confidence: float
    evidence_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ActuarialFact:
    """Staging fact row for actuarial metrics."""

    context: BitemporalFactContext
    metric_name: ActuarialMetricName
    metric_value: DualReportedValue
    confidence: float
    evidence_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AllocationFact:
    """Staging fact row for strategic allocation metrics."""

    context: BitemporalFactContext
    metric_name: AllocationMetricName
    metric_value: DualReportedValue
    confidence: float
    evidence_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class HoldingFact:
    """Staging fact row for manager/fund/vehicle holdings."""

    context: BitemporalFactContext
    manager_name: str | None
    fund_name: str | None
    vehicle_name: str | None
    metric_name: HoldingMetricName
    metric_value: DualReportedValue
    relationship_completeness: RelationshipCompleteness
    confidence: float
    evidence_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class FeeFact:
    """Staging fact row for fee observations."""

    context: BitemporalFactContext
    fee_category: FeeCategory
    manager_name: str | None
    metric_value: DualReportedValue
    confidence: float
    evidence_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CashFlowFact:
    """Staging fact row for plan-level cash-flow disclosures."""

    context: BitemporalFactContext
    beginning_aum: DualReportedValue
    ending_aum: DualReportedValue
    employer_contributions: DualReportedValue
    employee_contributions: DualReportedValue
    benefit_payments: DualReportedValue
    refunds: DualReportedValue
    confidence: float
    evidence_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ManagerFundVehicleRelationship:
    """Manager/fund/vehicle relationship row with completeness controls."""

    context: BitemporalFactContext
    manager_name: str
    fund_name: str | None
    vehicle_name: str | None
    relationship_completeness: RelationshipCompleteness
    known_not_invested: bool
    evidence_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ConsultantEngagementFact:
    """Consultant engagement and recommendation staging row."""

    context: BitemporalFactContext
    consultant_name: str
    role_description: str
    recommendation_topic: str | None
    recommendation_text: str | None
    attribution_outcome: str | None
    relationship_completeness: RelationshipCompleteness
    confidence: float
    evidence_refs: tuple[str, ...]


def query_bitemporal_as_of(
    facts: Sequence[TFact],
    *,
    effective_date: str,
    ingestion_date: str,
) -> list[TFact]:
    """Return rows valid at an as-of effective and ingestion timestamp."""
    as_of_effective = _parse_iso_temporal(effective_date, field_name="effective_date")
    as_of_ingestion = _parse_iso_temporal(ingestion_date, field_name="ingestion_date")

    eligible_rows: list[TFact] = []
    for row in facts:
        row_effective = _parse_iso_temporal(
            row.context.effective_date,
            field_name="row.context.effective_date",
        )
        row_ingestion = _parse_iso_temporal(
            row.context.ingestion_date,
            field_name="row.context.ingestion_date",
        )
        if row_effective <= as_of_effective and row_ingestion <= as_of_ingestion:
            eligible_rows.append(row)

    return sorted(
        eligible_rows,
        key=lambda row: (
            row.context.plan_id,
            row.context.plan_period,
            row.context.effective_date,
            row.context.ingestion_date,
            row.context.benchmark_version,
        ),
    )
