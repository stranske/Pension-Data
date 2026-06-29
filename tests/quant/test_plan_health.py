"""Tests for the plan-agnostic pension health scorecard."""

from __future__ import annotations

from pension_data.quant.plan_health import (
    HealthThresholds,
    PlanHealthInputs,
    negative_amortization,
    net_cash_flow_pct,
    score_plan_health,
    tread_water_gap_usd,
)


def _dim(card, name):
    return next(d for d in card.dimensions if d.name == name)


def test_tread_water_and_negative_amortization_and_cashflow_helpers() -> None:
    # contribution 100, normal cost 40, i=0.07, UAAL 1000 -> required 110 -> gap -10
    assert tread_water_gap_usd(100.0, 40.0, 0.07, 1000.0) == -10.0
    assert negative_amortization(50.0, 0.07, 1000.0) is True  # 50 < 70
    assert negative_amortization(80.0, 0.07, 1000.0) is False
    assert net_cash_flow_pct(50.0, 90.0, 1000.0) == -4.0
    # non-finite / zero guards
    assert tread_water_gap_usd(float("nan"), 40.0, 0.07, 1000.0) is None
    assert net_cash_flow_pct(1.0, 1.0, 0.0) is None
    assert negative_amortization(None, 0.07, 1000.0) is None


def test_funded_ratio_bands() -> None:
    base = {"plan_id": "P", "plan_period": "FY2024"}
    assert (
        _dim(
            score_plan_health(PlanHealthInputs(**base, funded_ratio_mva=0.95)), "funded_ratio_mva"
        ).rating
        == "green"
    )
    assert (
        _dim(
            score_plan_health(PlanHealthInputs(**base, funded_ratio_mva=0.80)), "funded_ratio_mva"
        ).rating
        == "yellow"
    )
    assert (
        _dim(
            score_plan_health(PlanHealthInputs(**base, funded_ratio_mva=0.60)), "funded_ratio_mva"
        ).rating
        == "red"
    )


def test_nan_inputs_are_unknown_never_green() -> None:
    card = score_plan_health(
        PlanHealthInputs(
            plan_id="P",
            plan_period="FY2024",
            funded_ratio_mva=float("nan"),
            assumed_return=float("inf"),
            net_cash_flow_pct=float("nan"),
        )
    )
    assert _dim(card, "funded_ratio_mva").rating == "unknown"
    assert _dim(card, "assumed_return").rating == "unknown"
    assert _dim(card, "cash_flow_maturity").rating == "unknown"
    # an all-unknown card is unknown overall, NOT green
    assert card.overall == "unknown"
    assert card.n_green == 0


def test_assumed_return_reasonableness() -> None:
    base = {"plan_id": "P", "plan_period": "FY2024"}
    # at/below peer median and realistic -> green
    g = _dim(
        score_plan_health(
            PlanHealthInputs(
                **base,
                assumed_return=0.062,
                peer_assumed_return_median=0.070,
                realistic_return=0.062,
            )
        ),
        "assumed_return",
    )
    assert g.rating == "green"
    # well above realistic -> red
    r = _dim(
        score_plan_health(
            PlanHealthInputs(
                **base,
                assumed_return=0.075,
                peer_assumed_return_median=0.070,
                realistic_return=0.060,
            )
        ),
        "assumed_return",
    )
    assert r.rating == "red"


def test_contribution_and_tread_water_dimensions() -> None:
    base = {"plan_id": "P", "plan_period": "FY2024"}
    c = _dim(
        score_plan_health(PlanHealthInputs(**base, adc_usd=100.0, actual_contribution_usd=85.0)),
        "contribution_sufficiency",
    )
    assert c.rating == "red"  # 85% < 90%
    tw = _dim(
        score_plan_health(
            PlanHealthInputs(
                **base,
                actual_contribution_usd=50.0,
                normal_cost_usd=40.0,
                assumed_return=0.07,
                uaal_usd=1000.0,
            )
        ),
        "tread_water",
    )
    # required = 40 + 0.07*1000 = 110; gap = 50 - 110 = -60; yellow floor = -11 -> red
    assert tw.rating == "red" and tw.value == -60.0
    # a small shortfall lands in the yellow band (gap -10 >= floor -11)
    tw_yellow = _dim(
        score_plan_health(
            PlanHealthInputs(
                **base,
                actual_contribution_usd=100.0,
                normal_cost_usd=40.0,
                assumed_return=0.07,
                uaal_usd=1000.0,
            )
        ),
        "tread_water",
    )
    assert tw_yellow.rating == "yellow" and tw_yellow.value == -10.0


def test_gasb_crossover_and_amortization_and_overall() -> None:
    base = {"plan_id": "P", "plan_period": "FY2024"}
    x = _dim(
        score_plan_health(PlanHealthInputs(**base, gasb_discount_rate=0.055, assumed_return=0.070)),
        "gasb_crossover",
    )
    assert x.rating == "red"
    amort = _dim(
        score_plan_health(
            PlanHealthInputs(
                **base, amortization_payment_usd=50.0, assumed_return=0.07, uaal_usd=1000.0
            )
        ),
        "amortization",
    )
    assert amort.rating == "red"  # negative amortization
    # overall is red if any red, else yellow if any yellow, else green
    healthy = score_plan_health(
        PlanHealthInputs(
            **base,
            funded_ratio_mva=0.95,
            funded_ratio_trend=0.03,
            assumed_return=0.060,
            peer_assumed_return_median=0.070,
            realistic_return=0.062,
            adc_usd=100.0,
            actual_contribution_usd=100.0,
            normal_cost_usd=40.0,
            uaal_usd=100.0,
            amortization_is_closed=True,
            net_cash_flow_pct=-2.0,
            gasb_discount_rate=0.060,
            mortality_table_year=2010,
        )
    )
    assert healthy.overall == "green" and healthy.n_red == 0 and healthy.n_unknown == 0


def test_thresholds_are_configurable() -> None:
    base = {"plan_id": "P", "plan_period": "FY2024"}
    strict = HealthThresholds(funded_green=0.99)
    d = _dim(
        score_plan_health(PlanHealthInputs(**base, funded_ratio_mva=0.95), strict),
        "funded_ratio_mva",
    )
    assert d.rating == "yellow"  # 0.95 < 0.99 under stricter band
