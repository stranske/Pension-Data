"""Execution wrappers for canonical saved analytical views."""

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from collections.abc import Callable

from pension_data.quant.peer_stats import benchmark_metric
from pension_data.quant.plan_health import PlanHealthInputs, score_plan_health
from pension_data.query.saved_views.models import (
    AllocationPeerInput,
    AllocationPeerRow,
    BenchmarkPanelInput,
    BenchmarkPanelRow,
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
            for row in sorted(
                counterparty_rows, key=lambda item: (item.manager_name, item.fund_name)
            )
        }

        all_positions = sorted(set(subject_index) | set(counterparty_index))
        for position_key in all_positions:
            subject = subject_index.get(position_key)
            counterparty = counterparty_index.get(position_key)
            subject_state = subject.disclosure_state if subject else "not_disclosed"
            counterparty_state = counterparty.disclosure_state if counterparty else "not_disclosed"

            overlap_status: OverlapStatus = "unknown_due_to_non_disclosure"
            overlap_usd: float | None = None
            if subject_state == "disclosed" and counterparty_state == "disclosed":
                subject_exposure = subject.exposure_usd if subject is not None else None
                counterparty_exposure = (
                    counterparty.exposure_usd if counterparty is not None else None
                )
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


MetricGetter = Callable[[BenchmarkPanelInput], float | None]


_BENCHMARK_METRICS: tuple[tuple[str, MetricGetter, bool], ...] = (
    ("funded_ratio_ava", lambda row: row.funded_ratio_ava, True),
    ("funded_ratio_mva", lambda row: row.funded_ratio_mva, True),
    ("aal_usd", lambda row: row.aal_usd, False),
    ("uaal_usd", lambda row: row.uaal_usd, False),
    ("assumed_return", lambda row: row.assumed_return, False),
    ("discount_rate", lambda row: row.discount_rate, False),
    ("inflation_rate", lambda row: row.inflation_rate, False),
    ("payroll_growth_rate", lambda row: row.payroll_growth_rate, True),
    (
        "adc_vs_actual_contribution_ratio",
        lambda row: _ratio(row.actual_contribution_usd, row.adc_usd),
        True,
    ),
    (
        "contribution_pct_of_payroll",
        lambda row: _ratio(row.actual_contribution_usd, row.payroll_usd),
        True,
    ),
    ("net_return_1yr", lambda row: row.net_return_1yr, True),
    ("net_return_3yr", lambda row: row.net_return_3yr, True),
    ("net_return_5yr", lambda row: row.net_return_5yr, True),
    ("net_return_10yr", lambda row: row.net_return_10yr, True),
    ("net_external_cash_flow_pct", lambda row: row.net_external_cash_flow_pct, True),
    ("support_ratio", lambda row: row.support_ratio, True),
    ("benefit_payments_pct", lambda row: row.benefit_payments_pct, False),
    ("assets_payroll_ratio", lambda row: row.assets_payroll_ratio, True),
)


def _is_finite(value: float | None) -> bool:
    return value is not None and math.isfinite(value)


def _round_optional(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, 6)


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if not (_is_finite(numerator) and _is_finite(denominator)) or denominator == 0.0:
        return None
    return round(numerator / denominator, 6)  # type: ignore[operator]


def _delta(value: float | None, comparator: float | None) -> float | None:
    if not (_is_finite(value) and _is_finite(comparator)):
        return None
    return round(value - comparator, 6)  # type: ignore[operator]


def _amortization_is_closed(method: str | None) -> bool | None:
    if method is None:
        return None
    normalized = method.strip().lower()
    if not normalized:
        return None
    if "open" in normalized:
        return False
    if "closed" in normalized or "layer" in normalized:
        return True
    return None


def _health_by_metric(
    subject: BenchmarkPanelInput,
    *,
    peer_assumed_return_median: float | None = None,
) -> dict[str, tuple[str, str]]:
    scorecard = score_plan_health(
        PlanHealthInputs(
            plan_id=subject.plan_id,
            plan_period=subject.plan_period,
            funded_ratio_mva=subject.funded_ratio_mva,
            assumed_return=subject.assumed_return,
            peer_assumed_return_median=peer_assumed_return_median,
            realistic_return=subject.realistic_return,
            gasb_discount_rate=subject.discount_rate,
            adc_usd=subject.adc_usd,
            actual_contribution_usd=subject.actual_contribution_usd,
            normal_cost_usd=subject.normal_cost_usd,
            uaal_usd=subject.uaal_usd,
            amortization_payment_usd=subject.amortization_payment_usd,
            amortization_is_closed=_amortization_is_closed(subject.amortization_method),
            amortization_period_years=subject.amortization_period_years,
            net_cash_flow_pct=subject.net_external_cash_flow_pct,
            mortality_table_year=subject.mortality_table_year,
        )
    )
    return {
        "funded_ratio_mva": (
            scorecard.dimensions[0].rating,
            scorecard.dimensions[0].basis,
        ),
        "funded_ratio_trend": (
            scorecard.dimensions[1].rating,
            scorecard.dimensions[1].basis,
        ),
        "assumed_return": (
            scorecard.dimensions[2].rating,
            scorecard.dimensions[2].basis,
        ),
        "adc_vs_actual_contribution_ratio": (
            scorecard.dimensions[3].rating,
            scorecard.dimensions[3].basis,
        ),
        "net_external_cash_flow_pct": (
            scorecard.dimensions[6].rating,
            scorecard.dimensions[6].basis,
        ),
    }


def execute_benchmark_panel_view(
    rows: list[BenchmarkPanelInput],
    *,
    subject_plan_id: str,
    plan_period: str,
    tight_peer_group: str | None = None,
) -> list[BenchmarkPanelRow]:
    """Execute a subject-plan benchmark panel with peer stats and health ratings."""
    period_rows = [row for row in rows if row.plan_period == plan_period]
    subject = next((row for row in period_rows if row.plan_id == subject_plan_id), None)
    if subject is None:
        return []

    broad_peers = [row for row in period_rows if row.plan_id != subject_plan_id]
    tight_group = tight_peer_group or subject.peer_group
    tight_peers = [row for row in broad_peers if row.peer_group == tight_group]
    peer_assumed_returns = [
        value
        for row in broad_peers
        if (value := row.assumed_return) is not None and math.isfinite(value)
    ]
    peer_assumed_return_median = (
        round(statistics.median(peer_assumed_returns), 6) if peer_assumed_returns else None
    )
    health_by_metric = _health_by_metric(
        subject,
        peer_assumed_return_median=peer_assumed_return_median,
    )

    output: list[BenchmarkPanelRow] = []
    for metric_name, getter, higher_is_better in _BENCHMARK_METRICS:
        subject_value = _round_optional(getter(subject))
        peer_values = [value for row in broad_peers if (value := getter(row)) is not None]
        tight_values = [value for row in tight_peers if (value := getter(row)) is not None]
        peer_result = benchmark_metric(
            metric_name,
            subject_value,
            peer_values,
            higher_is_better=higher_is_better,
        )
        tight_result = benchmark_metric(
            metric_name,
            subject_value,
            tight_values,
            higher_is_better=higher_is_better,
        )
        health_rating, health_basis = health_by_metric.get(metric_name, (None, None))
        output.append(
            BenchmarkPanelRow(
                plan_id=subject.plan_id,
                plan_period=subject.plan_period,
                metric_name=metric_name,
                metric_value=subject_value,
                peer_percentile=peer_result.percentile,
                peer_z_score=peer_result.z_score,
                peer_median=peer_result.peer_median,
                delta_vs_peer_median=_delta(subject_value, peer_result.peer_median),
                delta_vs_assumed_return=(
                    _delta(subject_value, subject.assumed_return)
                    if metric_name.startswith("net_return_")
                    else None
                ),
                delta_vs_policy_benchmark=(
                    _delta(subject_value, subject.policy_benchmark_return)
                    if metric_name.startswith("net_return_")
                    else None
                ),
                tight_peer_percentile=tight_result.percentile,
                tight_peer_z_score=tight_result.z_score,
                health_rating=health_rating,
                health_basis=health_basis,
            )
        )

    return output
