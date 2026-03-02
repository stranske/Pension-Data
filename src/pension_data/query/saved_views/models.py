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
