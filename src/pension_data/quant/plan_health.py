"""Plan-agnostic pension health scorecard.

Synthesises the standard actuarial "is this plan healthy and are its assumptions
honest" tests into a 9-dimension Green/Yellow/Red scorecard. Every dimension reports
the numeric basis behind its rating (never an opaque score), and every threshold is
configurable via :class:`HealthThresholds` so the bands are explicit and tunable
rather than hard-coded magic numbers.

All inputs are optional because data availability varies by plan/source; a missing or
non-finite input yields an ``unknown`` rating for that dimension (it never silently
scores green). Works for any plan — the caller supplies the plan's figures plus, where
available, peer context (e.g. the NASRA peer-median assumed return) and a realistic
forward return for the plan's own asset mix.

Unit conventions (documented per field): funded ratios are ratios in [0, 1]; rates
(assumed return, discount rate, interest) are decimals (0.07 == 7%); USD figures are
plain floats; cash-flow maturity is a percent (e.g. -4.0 == -4%).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal

__all__ = [
    "RAG",
    "DimensionScore",
    "HealthThresholds",
    "PlanHealthInputs",
    "PlanHealthScorecard",
    "negative_amortization",
    "net_cash_flow_pct",
    "score_plan_health",
    "tread_water_gap_usd",
]

RAG = Literal["green", "yellow", "red", "unknown"]


def _is_num(value: float | None) -> bool:
    return value is not None and math.isfinite(value)


def _is_bounded(value: float | None, *, min_value: float, max_value: float) -> bool:
    return _is_num(value) and min_value <= value <= max_value  # type: ignore[operator]


def _is_nonnegative(value: float | None) -> bool:
    return _is_num(value) and value >= 0.0  # type: ignore[operator]


@dataclass(frozen=True, slots=True)
class HealthThresholds:
    """Configurable Green/Yellow/Red bands. Defaults are defensible public-plan
    conventions (see ``2026-06-28-R4-actuarial-analytics.md``); override to tune."""

    funded_green: float = 0.90
    funded_yellow: float = 0.70
    # assumed-return reasonableness, in decimal rate points above the comparator
    assumed_red_excess_vs_peer: float = 0.005
    assumed_red_excess_vs_realistic: float = 0.010
    assumed_yellow_excess_vs_realistic: float = 0.000
    # contribution sufficiency = actual / ADC
    adc_green: float = 1.00
    adc_yellow: float = 0.90
    # tread-water: contribution vs (normal cost + interest*UAAL); yellow band is a
    # shortfall up to this fraction of the tread-water requirement
    tread_water_yellow_shortfall_frac: float = 0.10
    # net external cash flow as % of assets
    cashflow_green_pct: float = -3.0
    cashflow_yellow_pct: float = -5.0
    # funded-ratio trend (signed change over the series)
    trend_flat_band: float = 0.01
    # amortization period (years) considered "long" when open/level-percent
    amortization_long_years: float = 25.0
    # mortality table base year
    mortality_green_year: int = 2010
    mortality_yellow_year: int = 2000


@dataclass(frozen=True, slots=True)
class PlanHealthInputs:
    """All-optional inputs for the scorecard (supply what you have)."""

    plan_id: str
    plan_period: str
    funded_ratio_mva: float | None = None  # ratio [0,1], market-value basis
    funded_ratio_trend: float | None = None  # signed change over the series
    assumed_return: float | None = None  # decimal, e.g. 0.07
    peer_assumed_return_median: float | None = None  # decimal
    realistic_return: float | None = None  # decimal, forward return for the asset mix
    gasb_discount_rate: float | None = None  # decimal; < assumed signals crossover
    adc_usd: float | None = None  # actuarially determined contribution
    actual_contribution_usd: float | None = None
    normal_cost_usd: float | None = None
    uaal_usd: float | None = None
    amortization_payment_usd: float | None = None
    amortization_is_closed: bool | None = None  # closed/layered == True
    amortization_period_years: float | None = None
    net_cash_flow_pct: float | None = None  # percent; or derive via net_cash_flow_pct()
    mortality_table_year: int | None = None


@dataclass(frozen=True, slots=True)
class DimensionScore:
    name: str
    rating: RAG
    value: float | None
    basis: str


@dataclass(frozen=True, slots=True)
class PlanHealthScorecard:
    plan_id: str
    plan_period: str
    dimensions: tuple[DimensionScore, ...]
    overall: RAG
    n_green: int
    n_yellow: int
    n_red: int
    n_unknown: int


def tread_water_gap_usd(
    contribution_usd: float | None,
    normal_cost_usd: float | None,
    interest_rate: float | None,
    uaal_usd: float | None,
) -> float | None:
    """Contribution minus the tread-water requirement (normal cost + interest on UAAL).

    Negative => contributions do not even cover interest accrual, so the UAAL grows
    before any investment experience. Returns ``None`` if any input is non-finite.
    """
    if not (
        _is_num(contribution_usd)
        and _is_num(normal_cost_usd)
        and _is_num(interest_rate)
        and _is_num(uaal_usd)
    ):
        return None
    required = normal_cost_usd + interest_rate * uaal_usd  # type: ignore[operator]
    return round(contribution_usd - required, 6)  # type: ignore[operator]


def negative_amortization(
    amortization_payment_usd: float | None,
    interest_rate: float | None,
    uaal_usd: float | None,
) -> bool | None:
    """True when the amortization payment is less than interest accrual on the UAAL."""
    if not (_is_num(amortization_payment_usd) and _is_num(interest_rate) and _is_num(uaal_usd)):
        return None
    return amortization_payment_usd < interest_rate * uaal_usd  # type: ignore[operator]


def net_cash_flow_pct(
    contributions_usd: float | None,
    benefit_payments_usd: float | None,
    market_assets_usd: float | None,
) -> float | None:
    """Net external cash flow as a percent of assets: (contrib - benefits) / assets.

    Returns ``None`` if any input is non-finite or assets are zero.
    """
    if not (
        _is_num(contributions_usd) and _is_num(benefit_payments_usd) and _is_num(market_assets_usd)
    ):
        return None
    if market_assets_usd == 0.0:
        return None
    return round(100.0 * (contributions_usd - benefit_payments_usd) / market_assets_usd, 4)  # type: ignore[operator]


def _funded(value: float | None, t: HealthThresholds) -> DimensionScore:
    if not _is_bounded(value, min_value=0.0, max_value=1.0):
        return DimensionScore(
            "funded_ratio_mva", "unknown", None, "no valid funded ratio (MVA) in [0, 1]"
        )
    if value >= t.funded_green:  # type: ignore[operator]
        rating: RAG = "green"
    elif value >= t.funded_yellow:  # type: ignore[operator]
        rating = "yellow"
    else:
        rating = "red"
    return DimensionScore(
        "funded_ratio_mva",
        rating,
        round(value, 6),  # type: ignore[arg-type]
        f"MVA funded ratio {value:.1%}; bands >={t.funded_green:.0%}/>={t.funded_yellow:.0%}",
    )


def _trend(value: float | None, t: HealthThresholds) -> DimensionScore:
    if not _is_bounded(value, min_value=-1.0, max_value=1.0):
        return DimensionScore("funded_ratio_trend", "unknown", None, "no valid trend in [-1, 1]")
    if value > t.trend_flat_band:  # type: ignore[operator]
        rating: RAG = "green"
    elif value >= -t.trend_flat_band:  # type: ignore[operator]
        rating = "yellow"
    else:
        rating = "red"
    return DimensionScore(
        "funded_ratio_trend",
        rating,
        round(value, 6),  # type: ignore[arg-type]
        f"funded-ratio change {value:+.3f}; flat band +/-{t.trend_flat_band}",
    )


def _assumed_return(inp: PlanHealthInputs, t: HealthThresholds) -> DimensionScore:
    rate = inp.assumed_return
    if not _is_bounded(rate, min_value=0.0, max_value=1.0):
        return DimensionScore(
            "assumed_return", "unknown", None, "no valid assumed return in [0, 1]"
        )
    peer = inp.peer_assumed_return_median
    realistic = inp.realistic_return
    reasons: list[str] = []
    rating: RAG = "green"
    if realistic is not None and not _is_bounded(realistic, min_value=0.0, max_value=1.0):
        return DimensionScore(
            "assumed_return", "unknown", round(rate, 6), "realistic return outside [0, 1]"
        )
    if peer is not None and not _is_bounded(peer, min_value=0.0, max_value=1.0):
        return DimensionScore(
            "assumed_return", "unknown", round(rate, 6), "peer assumed return outside [0, 1]"
        )
    if _is_bounded(realistic, min_value=0.0, max_value=1.0):
        excess_real = rate - realistic  # type: ignore[operator]
        reasons.append(f"vs realistic {realistic:.2%}: {excess_real:+.2%}")
        if excess_real > t.assumed_red_excess_vs_realistic:
            rating = "red"
        elif excess_real > t.assumed_yellow_excess_vs_realistic:
            rating = "yellow"
    if _is_bounded(peer, min_value=0.0, max_value=1.0):
        excess_peer = rate - peer  # type: ignore[operator]
        reasons.append(f"vs peer median {peer:.2%}: {excess_peer:+.2%}")
        if excess_peer > t.assumed_red_excess_vs_peer:
            rating = "red"
    if not reasons:
        return DimensionScore(
            "assumed_return",
            "unknown",
            round(rate, 6),  # type: ignore[arg-type]
            f"assumed {rate:.2%}; no peer/realistic comparator supplied",
        )
    return DimensionScore(
        "assumed_return",
        rating,
        round(rate, 6),  # type: ignore[arg-type]
        f"assumed {rate:.2%}; " + "; ".join(reasons),
    )


def _contribution(inp: PlanHealthInputs, t: HealthThresholds) -> DimensionScore:
    adc, actual = inp.adc_usd, inp.actual_contribution_usd
    if not (_is_nonnegative(adc) and _is_nonnegative(actual)) or adc == 0.0:
        return DimensionScore(
            "contribution_sufficiency", "unknown", None, "no valid non-negative ADC/actual"
        )
    ratio = actual / adc  # type: ignore[operator]
    if ratio >= t.adc_green:
        rating: RAG = "green"
    elif ratio >= t.adc_yellow:
        rating = "yellow"
    else:
        rating = "red"
    return DimensionScore(
        "contribution_sufficiency",
        rating,
        round(ratio, 4),
        f"paid {ratio:.0%} of ADC; bands >={t.adc_green:.0%}/>={t.adc_yellow:.0%}",
    )


def _tread_water(inp: PlanHealthInputs, t: HealthThresholds) -> DimensionScore:
    if not (
        _is_nonnegative(inp.actual_contribution_usd)
        and _is_nonnegative(inp.normal_cost_usd)
        and _is_bounded(inp.assumed_return, min_value=0.0, max_value=1.0)
        and _is_nonnegative(inp.uaal_usd)
    ):
        return DimensionScore("tread_water", "unknown", None, "insufficient valid inputs")
    gap = tread_water_gap_usd(
        inp.actual_contribution_usd, inp.normal_cost_usd, inp.assumed_return, inp.uaal_usd
    )
    if gap is None:
        return DimensionScore("tread_water", "unknown", None, "insufficient inputs")
    required = (inp.normal_cost_usd or 0.0) + (inp.assumed_return or 0.0) * (inp.uaal_usd or 0.0)
    yellow_floor = -abs(required) * t.tread_water_yellow_shortfall_frac
    if gap >= 0.0:
        rating: RAG = "green"
    elif gap >= yellow_floor:
        rating = "yellow"
    else:
        rating = "red"
    return DimensionScore(
        "tread_water",
        rating,
        gap,
        f"contribution minus (normal cost + i*UAAL) = {gap:,.0f}",
    )


def _amortization(inp: PlanHealthInputs, t: HealthThresholds) -> DimensionScore:
    if not (
        (inp.amortization_payment_usd is None or _is_nonnegative(inp.amortization_payment_usd))
        and (
            inp.assumed_return is None
            or _is_bounded(inp.assumed_return, min_value=0.0, max_value=1.0)
        )
        and (inp.uaal_usd is None or _is_nonnegative(inp.uaal_usd))
        and (
            inp.amortization_period_years is None or _is_nonnegative(inp.amortization_period_years)
        )
    ):
        return DimensionScore("amortization", "unknown", None, "invalid amortization inputs")
    neg = negative_amortization(inp.amortization_payment_usd, inp.assumed_return, inp.uaal_usd)
    if neg:
        return DimensionScore(
            "amortization", "red", None, "negative amortization: payment < interest on UAAL"
        )
    if inp.amortization_is_closed is None:
        if neg is None:
            return DimensionScore("amortization", "unknown", None, "no amortization inputs")
        return DimensionScore("amortization", "green", None, "not negatively amortizing")
    if inp.amortization_is_closed:
        return DimensionScore("amortization", "green", None, "closed/layered amortization")
    period = inp.amortization_period_years
    if _is_num(period) and period > t.amortization_long_years:  # type: ignore[operator]
        return DimensionScore(
            "amortization",
            "yellow",
            round(period, 1),  # type: ignore[arg-type]
            f"open amortization over {period:.0f}y (> {t.amortization_long_years:.0f}y)",
        )
    return DimensionScore("amortization", "yellow", None, "open amortization")


def _cash_flow(inp: PlanHealthInputs, t: HealthThresholds) -> DimensionScore:
    value = inp.net_cash_flow_pct
    if not _is_bounded(value, min_value=-100.0, max_value=100.0):
        return DimensionScore(
            "cash_flow_maturity", "unknown", None, "no valid net cash flow % in [-100, 100]"
        )
    if value >= t.cashflow_green_pct:  # type: ignore[operator]
        rating: RAG = "green"
    elif value >= t.cashflow_yellow_pct:  # type: ignore[operator]
        rating = "yellow"
    else:
        rating = "red"
    return DimensionScore(
        "cash_flow_maturity",
        rating,
        round(value, 4),  # type: ignore[arg-type]
        f"net external cash flow {value:+.1f}% of assets; bands >={t.cashflow_green_pct}/>={t.cashflow_yellow_pct}",
    )


def _gasb_crossover(inp: PlanHealthInputs) -> DimensionScore:
    gasb, assumed = inp.gasb_discount_rate, inp.assumed_return
    if not (
        _is_bounded(gasb, min_value=0.0, max_value=1.0)
        and _is_bounded(assumed, min_value=0.0, max_value=1.0)
    ):
        return DimensionScore("gasb_crossover", "unknown", None, "no valid GASB/assumed rate")
    if gasb < assumed:  # type: ignore[operator]
        return DimensionScore(
            "gasb_crossover",
            "red",
            round(gasb, 6),  # type: ignore[arg-type]
            f"GASB discount {gasb:.2%} < assumed {assumed:.2%}: crossover/depletion (blended rate)",
        )
    return DimensionScore(
        "gasb_crossover",
        "green",
        round(gasb, 6),  # type: ignore[arg-type]
        f"GASB discount {gasb:.2%} >= assumed {assumed:.2%}: no crossover",
    )


def _mortality(inp: PlanHealthInputs, t: HealthThresholds) -> DimensionScore:
    year = inp.mortality_table_year
    if year is None:
        return DimensionScore("mortality_currency", "unknown", None, "no mortality table year")
    if year >= t.mortality_green_year:
        rating: RAG = "green"
    elif year >= t.mortality_yellow_year:
        rating = "yellow"
    else:
        rating = "red"
    return DimensionScore(
        "mortality_currency",
        rating,
        float(year),
        f"mortality base year {year}; green >={t.mortality_green_year}",
    )


def score_plan_health(
    inputs: PlanHealthInputs, thresholds: HealthThresholds | None = None
) -> PlanHealthScorecard:
    """Compute the 9-dimension Green/Yellow/Red scorecard for one plan.

    Overall rating: ``red`` if any dimension is red, else ``yellow`` if any is yellow,
    else ``green`` if at least one dimension scored, else ``unknown``. Unknown
    dimensions never count as green.
    """
    t = thresholds or HealthThresholds()
    dims = (
        _funded(inputs.funded_ratio_mva, t),
        _trend(inputs.funded_ratio_trend, t),
        _assumed_return(inputs, t),
        _contribution(inputs, t),
        _tread_water(inputs, t),
        _amortization(inputs, t),
        _cash_flow(inputs, t),
        _gasb_crossover(inputs),
        _mortality(inputs, t),
    )
    n_green = sum(1 for d in dims if d.rating == "green")
    n_yellow = sum(1 for d in dims if d.rating == "yellow")
    n_red = sum(1 for d in dims if d.rating == "red")
    n_unknown = sum(1 for d in dims if d.rating == "unknown")
    if n_red:
        overall: RAG = "red"
    elif n_yellow:
        overall = "yellow"
    elif n_green:
        overall = "green"
    else:
        overall = "unknown"
    return PlanHealthScorecard(
        plan_id=inputs.plan_id,
        plan_period=inputs.plan_period,
        dimensions=dims,
        overall=overall,
        n_green=n_green,
        n_yellow=n_yellow,
        n_red=n_red,
        n_unknown=n_unknown,
    )
