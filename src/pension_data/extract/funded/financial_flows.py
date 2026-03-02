"""AUM and external cash-flow extraction with deterministic derived metrics."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal

from pension_data.db.models.financial_flows import FinancialDisclosureLevel, PlanFinancialFlow
from pension_data.normalize.financial_units import (
    UnitScale,
    normalize_flow_sign,
    normalize_money_to_usd,
)

FinancialFlowWarningCode = Literal["partial_disclosure", "not_disclosed", "consistency_gap"]

_WARNING_MESSAGES: dict[FinancialFlowWarningCode, str] = {
    "partial_disclosure": "Financial flow disclosure is partial for this plan-period.",
    "not_disclosed": "Financial flow disclosure is not available for this plan-period.",
    "consistency_gap": "Beginning AUM + external flow does not reconcile to ending AUM.",
}


@dataclass(frozen=True, slots=True)
class RawFinancialFlowInput:
    """Raw statement values before unit and sign normalization."""

    source_document_id: str
    source_url: str
    effective_period: str
    reported_at: str
    unit_scale: UnitScale
    outflows_reported_as_negative: bool
    beginning_aum: float | None
    ending_aum: float | None
    employer_contributions: float | None
    employee_contributions: float | None
    benefit_payments: float | None
    refunds: float | None
    evidence_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class FinancialFlowWarning:
    """Extraction warning for flow completeness or consistency issues."""

    code: FinancialFlowWarningCode
    plan_id: str
    plan_period: str
    message: str
    evidence_refs: tuple[str, ...]


def _dedupe_refs(evidence_refs: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(ref.strip() for ref in evidence_refs if ref.strip()))


def _disclosure_level(
    *,
    beginning_aum: float | None,
    ending_aum: float | None,
    employer_contributions: float | None,
    employee_contributions: float | None,
    benefit_payments: float | None,
    refunds: float | None,
) -> FinancialDisclosureLevel:
    fields = (
        beginning_aum,
        ending_aum,
        employer_contributions,
        employee_contributions,
        benefit_payments,
        refunds,
    )
    if all(value is None for value in fields):
        return "not_disclosed"
    if all(value is not None for value in fields):
        return "complete"
    return "partial"


def _sum_known(values: tuple[float | None, ...]) -> float | None:
    known_values = [value for value in values if value is not None]
    if not known_values:
        return None
    return round(sum(known_values), 6)


def _source_metadata(*, source_url: str, unit_scale: UnitScale) -> MappingProxyType[str, str]:
    return MappingProxyType({"source_url": source_url, "unit_scale": unit_scale})


def extract_plan_financial_flow(
    *,
    plan_id: str,
    plan_period: str,
    raw: RawFinancialFlowInput,
    consistency_tolerance_usd: float = 1.0,
) -> tuple[PlanFinancialFlow, list[FinancialFlowWarning]]:
    """Extract normalized plan financial flow row and deterministic warning set."""
    beginning_aum_usd = normalize_money_to_usd(raw.beginning_aum, unit_scale=raw.unit_scale)
    ending_aum_usd = normalize_money_to_usd(raw.ending_aum, unit_scale=raw.unit_scale)
    employer_contributions_usd = normalize_flow_sign(
        normalize_money_to_usd(raw.employer_contributions, unit_scale=raw.unit_scale),
        direction="inflow",
        outflows_reported_as_negative=raw.outflows_reported_as_negative,
    )
    employee_contributions_usd = normalize_flow_sign(
        normalize_money_to_usd(raw.employee_contributions, unit_scale=raw.unit_scale),
        direction="inflow",
        outflows_reported_as_negative=raw.outflows_reported_as_negative,
    )
    benefit_payments_usd = normalize_flow_sign(
        normalize_money_to_usd(raw.benefit_payments, unit_scale=raw.unit_scale),
        direction="outflow",
        outflows_reported_as_negative=raw.outflows_reported_as_negative,
    )
    refunds_usd = normalize_flow_sign(
        normalize_money_to_usd(raw.refunds, unit_scale=raw.unit_scale),
        direction="outflow",
        outflows_reported_as_negative=raw.outflows_reported_as_negative,
    )
    net_external_cash_flow_usd = _sum_known(
        (
            employer_contributions_usd,
            employee_contributions_usd,
            benefit_payments_usd,
            refunds_usd,
        )
    )
    if (
        beginning_aum_usd is None
        or beginning_aum_usd == 0.0
        or net_external_cash_flow_usd is None
    ):
        net_external_cash_flow_rate_pct = None
    else:
        net_external_cash_flow_rate_pct = round(
            (net_external_cash_flow_usd / beginning_aum_usd) * 100.0,
            6,
        )

    consistency_gap_usd: float | None = None
    if (
        beginning_aum_usd is not None
        and ending_aum_usd is not None
        and net_external_cash_flow_usd is not None
    ):
        expected_end = round(beginning_aum_usd + net_external_cash_flow_usd, 6)
        consistency_gap_usd = round(ending_aum_usd - expected_end, 6)

    disclosure_level = _disclosure_level(
        beginning_aum=beginning_aum_usd,
        ending_aum=ending_aum_usd,
        employer_contributions=employer_contributions_usd,
        employee_contributions=employee_contributions_usd,
        benefit_payments=benefit_payments_usd,
        refunds=refunds_usd,
    )
    warning_codes: list[FinancialFlowWarningCode] = []
    if disclosure_level == "partial":
        warning_codes.append("partial_disclosure")
    if disclosure_level == "not_disclosed":
        warning_codes.append("not_disclosed")
    if consistency_gap_usd is not None and abs(consistency_gap_usd) > consistency_tolerance_usd:
        warning_codes.append("consistency_gap")

    flow_row = PlanFinancialFlow(
        plan_id=plan_id,
        plan_period=plan_period,
        effective_period=raw.effective_period,
        reported_at=raw.reported_at,
        source_document_id=raw.source_document_id,
        beginning_aum_usd=beginning_aum_usd,
        ending_aum_usd=ending_aum_usd,
        employer_contributions_usd=employer_contributions_usd,
        employee_contributions_usd=employee_contributions_usd,
        benefit_payments_usd=benefit_payments_usd,
        refunds_usd=refunds_usd,
        net_external_cash_flow_usd=net_external_cash_flow_usd,
        net_external_cash_flow_rate_pct=net_external_cash_flow_rate_pct,
        consistency_gap_usd=consistency_gap_usd,
        disclosure_level=disclosure_level,
        evidence_refs=_dedupe_refs(raw.evidence_refs),
        source_metadata=_source_metadata(source_url=raw.source_url, unit_scale=raw.unit_scale),
    )
    warnings = [
        FinancialFlowWarning(
            code=code,
            plan_id=plan_id,
            plan_period=plan_period,
            message=_WARNING_MESSAGES[code],
            evidence_refs=flow_row.evidence_refs,
        )
        for code in warning_codes
    ]
    return flow_row, warnings
