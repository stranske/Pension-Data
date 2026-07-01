"""Typed records for saved analytical views."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

DisclosureState = Literal["disclosed", "not_disclosed", "known_not_invested"]
OverlapStatus = Literal["overlap", "known_not_invested", "unknown_due_to_non_disclosure"]


@dataclass(frozen=True, slots=True)
class FundingTrendInput:
    """Input row for funding trend view."""

    plan_id: str
    plan_period: str
    funded_ratio: float
    employer_contributions_usd: float | None
    employee_contributions_usd: float | None
    benefit_payments_usd: float | None


@dataclass(frozen=True, slots=True)
class FundingTrendRow:
    """Output row for funding trend view."""

    plan_id: str
    plan_period: str
    funded_ratio: float
    funded_ratio_change: float | None
    net_external_cash_flow_usd: float | None


@dataclass(frozen=True, slots=True)
class AllocationPeerInput:
    """Input row for allocation peer comparison view."""

    plan_id: str
    plan_period: str
    peer_group: str
    asset_class: str
    allocation_pct: float


@dataclass(frozen=True, slots=True)
class AllocationPeerRow:
    """Output row for allocation peer comparison view."""

    plan_id: str
    plan_period: str
    asset_class: str
    plan_allocation_pct: float
    peer_mean_pct: float | None
    peer_median_pct: float | None
    delta_vs_peer_mean_pct: float | None


@dataclass(frozen=True, slots=True)
class HoldingsOverlapInput:
    """Input row for holdings overlap view."""

    plan_id: str
    plan_period: str
    manager_name: str
    fund_name: str
    exposure_usd: float | None
    disclosure_state: DisclosureState


@dataclass(frozen=True, slots=True)
class HoldingsOverlapRow:
    """Output row for holdings overlap view."""

    subject_plan_id: str
    counterparty_plan_id: str
    plan_period: str
    manager_name: str
    fund_name: str
    overlap_status: OverlapStatus
    overlap_usd: float | None
    subject_disclosure_state: DisclosureState
    counterparty_disclosure_state: DisclosureState


@dataclass(frozen=True, slots=True)
class BenchmarkPanelInput:
    """Input row for a plan benchmark panel."""

    plan_id: str
    plan_period: str
    peer_group: str
    funded_ratio_ava: float | None = None
    funded_ratio_mva: float | None = None
    aal_usd: float | None = None
    uaal_usd: float | None = None
    assumed_return: float | None = None
    discount_rate: float | None = None
    inflation_rate: float | None = None
    payroll_growth_rate: float | None = None
    amortization_method: str | None = None
    amortization_period_years: float | None = None
    mortality_table_year: int | None = None
    adc_usd: float | None = None
    actual_contribution_usd: float | None = None
    payroll_usd: float | None = None
    normal_cost_usd: float | None = None
    amortization_payment_usd: float | None = None
    net_return_1yr: float | None = None
    net_return_3yr: float | None = None
    net_return_5yr: float | None = None
    net_return_10yr: float | None = None
    net_external_cash_flow_pct: float | None = None
    support_ratio: float | None = None
    benefit_payments_pct: float | None = None
    assets_payroll_ratio: float | None = None
    policy_benchmark_return: float | None = None
    realistic_return: float | None = None


@dataclass(frozen=True, slots=True)
class BenchmarkPanelRow:
    """One benchmark metric with peer statistics and health-score context."""

    plan_id: str
    plan_period: str
    metric_name: str
    metric_value: float | None
    peer_percentile: float | None
    peer_quartile_rank: int | None
    peer_z_score: float | None
    peer_median: float | None
    delta_vs_peer_median: float | None
    delta_vs_assumed_return: float | None
    delta_vs_policy_benchmark: float | None
    tight_peer_percentile: float | None
    tight_peer_quartile_rank: int | None
    tight_peer_z_score: float | None
    health_rating: str | None
    health_basis: str | None
    health_dimension_name: str | None = None
    health_dimension_value: float | None = None
