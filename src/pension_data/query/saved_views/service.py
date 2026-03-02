"""Execution wrappers for canonical saved analytical views."""

from __future__ import annotations

import statistics
from collections import defaultdict

from pension_data.query.saved_views.models import (
    AllocationPeerInput,
    AllocationPeerRow,
    FundingTrendInput,
    FundingTrendRow,
    HoldingsOverlapInput,
    HoldingsOverlapRow,
    OverlapStatus,
)


def _period_key(period: str) -> tuple[int, str]:
    digits = "".join(ch for ch in period if ch.isdigit())
    return (int(digits) if digits else -1, period)


def _net_external_flow(row: FundingTrendInput) -> float | None:
    components = [
        row.employer_contributions_usd,
        row.employee_contributions_usd,
        None if row.benefit_payments_usd is None else -row.benefit_payments_usd,
    ]
    known = [value for value in components if value is not None]
    if not known:
        return None
    return round(sum(known), 6)


def execute_funding_trend_view(rows: list[FundingTrendInput]) -> list[FundingTrendRow]:
    """Execute funding trend view over plan-period funding rows."""
    by_plan: dict[str, list[FundingTrendInput]] = defaultdict(list)
    for row in rows:
        by_plan[row.plan_id].append(row)

    output: list[FundingTrendRow] = []
    for plan_id in sorted(by_plan):
        previous_ratio: float | None = None
        for row in sorted(by_plan[plan_id], key=lambda item: _period_key(item.plan_period)):
            ratio_change = None
            if previous_ratio is not None:
                ratio_change = round(row.funded_ratio - previous_ratio, 6)
            output.append(
                FundingTrendRow(
                    plan_id=row.plan_id,
                    plan_period=row.plan_period,
                    funded_ratio=round(row.funded_ratio, 6),
                    funded_ratio_change=ratio_change,
                    net_external_cash_flow_usd=_net_external_flow(row),
                )
            )
            previous_ratio = row.funded_ratio
    return output


def execute_allocation_peer_compare_view(
    rows: list[AllocationPeerInput],
    *,
    subject_plan_id: str,
    plan_period: str,
) -> list[AllocationPeerRow]:
    """Execute allocation peer comparison for one plan and period."""
    period_rows = [row for row in rows if row.plan_period == plan_period]
    subject_rows = [row for row in period_rows if row.plan_id == subject_plan_id]

    output: list[AllocationPeerRow] = []
    for subject in sorted(subject_rows, key=lambda row: row.asset_class):
        peers = [
            row.allocation_pct
            for row in period_rows
            if row.plan_id != subject_plan_id
            and row.asset_class == subject.asset_class
            and row.peer_group == subject.peer_group
        ]
        peer_mean: float | None = None
        peer_median: float | None = None
        delta: float | None = None
        if peers:
            peer_mean = round(statistics.fmean(peers), 6)
            peer_median = round(statistics.median(peers), 6)
            delta = round(subject.allocation_pct - peer_mean, 6)

        output.append(
            AllocationPeerRow(
                plan_id=subject.plan_id,
                plan_period=subject.plan_period,
                asset_class=subject.asset_class,
                plan_allocation_pct=round(subject.allocation_pct, 6),
                peer_mean_pct=peer_mean,
                peer_median_pct=peer_median,
                delta_vs_peer_mean_pct=delta,
            )
        )

    return output


def execute_holdings_overlap_view(
    rows: list[HoldingsOverlapInput],
    *,
    subject_plan_id: str,
    plan_period: str,
) -> list[HoldingsOverlapRow]:
    """Execute coverage-aware holdings overlap between subject plan and peer plans."""
    period_rows = [row for row in rows if row.plan_period == plan_period]
    subject_rows = [row for row in period_rows if row.plan_id == subject_plan_id]

    subject_index = {
        (row.manager_name, row.fund_name): row
        for row in sorted(subject_rows, key=lambda item: (item.manager_name, item.fund_name))
    }
    counterparty_plans = sorted(
        {row.plan_id for row in period_rows if row.plan_id != subject_plan_id}
    )

    output: list[HoldingsOverlapRow] = []
    for counterparty_plan in counterparty_plans:
        counterparty_rows = [row for row in period_rows if row.plan_id == counterparty_plan]
        counterparty_index = {
            (row.manager_name, row.fund_name): row
            for row in sorted(counterparty_rows, key=lambda item: (item.manager_name, item.fund_name))
        }

        all_positions = sorted(set(subject_index) | set(counterparty_index))
        for position_key in all_positions:
            subject = subject_index.get(position_key)
            counterparty = counterparty_index.get(position_key)
            subject_state = subject.disclosure_state if subject else "known_not_invested"
            counterparty_state = counterparty.disclosure_state if counterparty else "known_not_invested"

            overlap_status: OverlapStatus = "unknown_due_to_non_disclosure"
            overlap_usd: float | None = None
            if subject_state == "disclosed" and counterparty_state == "disclosed":
                subject_exposure = subject.exposure_usd if subject is not None else None
                counterparty_exposure = counterparty.exposure_usd if counterparty is not None else None
                if subject_exposure is not None and counterparty_exposure is not None:
                    overlap_status = "overlap"
                    overlap_usd = round(min(subject_exposure, counterparty_exposure), 6)
                else:
                    overlap_status = "unknown_due_to_non_disclosure"
            elif "not_disclosed" not in {subject_state, counterparty_state}:
                overlap_status = "known_not_invested"

            manager_name = position_key[0]
            fund_name = position_key[1]
            output.append(
                HoldingsOverlapRow(
                    subject_plan_id=subject_plan_id,
                    counterparty_plan_id=counterparty_plan,
                    plan_period=plan_period,
                    manager_name=manager_name,
                    fund_name=fund_name,
                    overlap_status=overlap_status,
                    overlap_usd=overlap_usd,
                    subject_disclosure_state=subject_state,
                    counterparty_disclosure_state=counterparty_state,
                )
            )

    return output
