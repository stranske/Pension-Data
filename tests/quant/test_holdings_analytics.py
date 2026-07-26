"""Test gate for the holdings analytics layer (issue #648).

Fixed synthetic portfolio; asserts allocation-vs-policy drift, HHI/top-N,
active share, and the −20% equity funded-status stress — each with its scope
label — plus the finite-guard rejection behavior at every numeric boundary.

The stress assertion (:func:`test_equity_shock_reduces_funded_status`) is the
deliberate-break target: flipping ``EQUITY_SHOCK`` from −0.20 to +0.20 turns the
shock into a gain and makes the "funded status falls" assertion fail.
"""

from __future__ import annotations

import math

import pytest

from pension_data.db.models.investment_positions import PlanSecurityPosition
from pension_data.quant.holdings_analytics import (
    compute_active_share,
    compute_allocation_drift,
    compute_brinson_attribution,
    compute_concentration,
    compute_factor_tilts,
    compute_fee_effectiveness,
    compute_liquidity,
    compute_security_overlap,
    positions_to_weights,
    run_funded_status_combined_shock,
    run_funded_status_equity_shock,
)

# Deliberate-break knob: flip to +0.20 to break test_equity_shock_reduces_funded_status.
EQUITY_SHOCK = -0.20


# ---------------------------------------------------------------------------
# Fixed synthetic portfolio
# ---------------------------------------------------------------------------
def _equity_positions() -> list[PlanSecurityPosition]:
    """A fixed 5-name equity sleeve; market values chosen for round weights."""
    raw = [
        ("cusip:AAA", "Alpha Corp", 400.0),
        ("cusip:BBB", "Beta Corp", 300.0),
        ("cusip:CCC", "Gamma Corp", 150.0),
        ("cusip:DDD", "Delta Corp", 100.0),
        ("cusip:EEE", "Epsilon Corp", 50.0),
    ]
    return [
        PlanSecurityPosition(
            plan_id="CA-TEST",
            plan_period="FY2024",
            security_id=security_id,
            security_name=name,
            cusip=security_id.split(":", 1)[1],
            ticker=None,
            shares=None,
            market_value_usd=market_value,
            asset_class="public_equity",
            source="13f",
            as_of="2024-06-30",
            disclosure_state="disclosed",
            provenance_ref=f"prov:{security_id}",
            valid_from="2024-06-30",
            asserted_at="2024-06-30",
        )
        for security_id, name, market_value in raw
    ]


def test_positions_to_weights_normalizes_disclosed_universe() -> None:
    weights = positions_to_weights(_equity_positions())
    assert weights["cusip:AAA"] == pytest.approx(0.4)
    assert weights["cusip:BBB"] == pytest.approx(0.3)
    assert sum(weights.values()) == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Task 1 — allocation-vs-policy drift (scope-labeled)
# ---------------------------------------------------------------------------
def test_allocation_drift_total_plan_scope() -> None:
    report = compute_allocation_drift(
        plan_id="CA-TEST",
        plan_period="FY2024",
        actual_weights={
            "public_equity": 0.55,
            "fixed_income": 0.20,
            "private_equity": 0.15,
            "real_estate": 0.10,
        },
        target_weights={
            "public_equity": 0.50,
            "fixed_income": 0.25,
            "private_equity": 0.15,
            "real_estate": 0.10,
        },
        scope_label="total-plan",
        provenance_refs=["cafr:2024"],
    )
    # Only two classes move: +0.05 equity, -0.05 fixed income.
    # drift = 0.5 * (|0.05| + |0.05|) = 0.05
    assert report.drift == pytest.approx(0.05)
    assert report.scope_label == "total-plan"
    assert report.provenance_refs == ("cafr:2024",)
    # Alternatives (private_equity, real_estate) are on-target here.
    assert report.alternatives_active_weight == pytest.approx(0.0)
    equity_row = next(r for r in report.rows if r.asset_class == "public_equity")
    assert equity_row.active_weight == pytest.approx(0.05)


def test_allocation_drift_flags_alternatives_overweight() -> None:
    report = compute_allocation_drift(
        plan_id="CA-TEST",
        plan_period="FY2024",
        actual_weights={"public_equity": 0.55, "private_equity": 0.45},
        target_weights={"public_equity": 0.65, "private_equity": 0.35},
        scope_label="total-plan",
        provenance_refs=["cafr:2024"],
    )
    assert report.alternatives_active_weight == pytest.approx(0.10)
    pe_row = next(r for r in report.rows if r.asset_class == "private_equity")
    assert pe_row.is_alternative is True


# ---------------------------------------------------------------------------
# Task 5 — concentration HHI / top-N (equity-sleeve scope)
# ---------------------------------------------------------------------------
def test_concentration_hhi_and_top_n_equity_sleeve() -> None:
    weights = positions_to_weights(_equity_positions())
    report = compute_concentration(
        plan_id="CA-TEST",
        plan_period="FY2024",
        weights=weights,
        top_n=2,
        provenance_refs=["13f:2024q2"],
    )
    # HHI = 0.4^2 + 0.3^2 + 0.15^2 + 0.1^2 + 0.05^2
    #     = 0.16 + 0.09 + 0.0225 + 0.01 + 0.0025 = 0.285
    assert report.hhi == pytest.approx(0.285)
    assert report.effective_n == pytest.approx(1.0 / 0.285, rel=1e-5)
    # top-2 weight = 0.4 + 0.3 = 0.7
    assert report.top_n_weight == pytest.approx(0.7)
    assert report.n_holdings == 5
    assert report.scope_label == "equity-sleeve"


# ---------------------------------------------------------------------------
# Task 4 — active share (Cremers-Petajisto bands) + overlap (equity-sleeve)
# ---------------------------------------------------------------------------
def test_active_share_bands_and_fee_per_unit() -> None:
    # Portfolio holds two names the benchmark does not; benchmark holds two the
    # portfolio does not. Active share = 0.5 * sum|dw|.
    portfolio = {"AAA": 0.5, "BBB": 0.5}
    benchmark = {"AAA": 0.25, "BBB": 0.25, "CCC": 0.25, "DDD": 0.25}
    report = compute_active_share(
        plan_id="CA-TEST",
        plan_period="FY2024",
        portfolio_weights=portfolio,
        benchmark_weights=benchmark,
        fee_bps=60.0,
        counterparty_weights={"AAA": 0.5, "CCC": 0.5},
        provenance_refs=["13f:2024q2"],
    )
    # |0.5-0.25|*2 + |0-0.25|*2 = 0.25*4 = 1.0 -> active share = 0.5
    assert report.active_share == pytest.approx(0.5)
    assert report.active_share_pct == pytest.approx(50.0)
    assert report.cremers_band == "moderately-active"
    # fee per unit of active risk = 60 bps / 50 pts = 1.2
    assert report.fee_per_unit_active_risk == pytest.approx(1.2)
    # pairwise overlap vs counterparty {AAA:0.5, CCC:0.5} = min(0.5,0.5)+min(0.5,0)+... = 0.5
    assert report.pairwise_overlap == pytest.approx(0.5)
    assert report.scope_label == "equity-sleeve"


def test_active_share_closet_and_high_bands() -> None:
    closet = compute_active_share(
        plan_id="p",
        plan_period="FY2024",
        portfolio_weights={"AAA": 0.5, "BBB": 0.5},
        benchmark_weights={"AAA": 0.55, "BBB": 0.45},
        provenance_refs=[],
    )
    assert closet.cremers_band == "closet-indexer"  # AS = 5%
    high = compute_active_share(
        plan_id="p",
        plan_period="FY2024",
        portfolio_weights={"AAA": 1.0},
        benchmark_weights={"BBB": 1.0},
        provenance_refs=[],
    )
    assert high.active_share_pct == pytest.approx(100.0)
    assert high.cremers_band == "very-highly-active"


def test_security_overlap_min_exposure_seam() -> None:
    rows, overlap = compute_security_overlap(
        subject_weights={"AAA": 0.6, "BBB": 0.4},
        counterparty_weights={"AAA": 0.3, "CCC": 0.7},
    )
    # min(0.6,0.3) + min(0.4,0) + min(0,0.7) = 0.3
    assert overlap == pytest.approx(0.3)
    aaa = next(r for r in rows if r.security_id == "AAA")
    assert aaa.overlap_weight == pytest.approx(0.3)


# ---------------------------------------------------------------------------
# Task 2 — funded-status stress (via scenarios.py) — DELIBERATE-BREAK TARGET
# ---------------------------------------------------------------------------
def test_equity_shock_reduces_funded_status() -> None:
    result = run_funded_status_equity_shock(
        plan_id="CA-TEST",
        plan_period="FY2024",
        total_assets_usd=100_000_000.0,
        liabilities_usd=125_000_000.0,
        equity_market_value_usd=40_000_000.0,
        equity_basis_scope="total-plan",
        equity_shock_pct=EQUITY_SHOCK,
        module_version="v0.1.0",
        source_snapshot_id="snapshot:2024-06-30",
        provenance_refs=["cafr:2024"],
    )
    # baseline funded ratio = 100M / 125M = 0.8
    assert result.baseline_funded_ratio == pytest.approx(0.8)
    # -20% on 40M equity = -8M assets -> 92M / 125M = 0.736
    assert result.asset_delta_usd == pytest.approx(-8_000_000.0)
    assert result.shocked_funded_ratio == pytest.approx(0.736)
    # THE stress assertion: a -20% equity shock must REDUCE funded status.
    # Deliberate-break: EQUITY_SHOCK = +0.20 makes this fail.
    assert result.shocked_funded_ratio < result.baseline_funded_ratio
    assert result.funded_ratio_delta < 0.0
    assert result.scope_label == "total-plan"
    # Routed through the shared scenario engine (reproducibility metadata present).
    assert result.scenario.mode == "deterministic"
    assert result.scenario.reproducibility.config_hash


def test_equity_sleeve_basis_downgrades_scope_label() -> None:
    """A 13F-sleeve equity basis must NOT yield a total-plan funded-status label."""
    result = run_funded_status_equity_shock(
        plan_id="CA-TEST",
        plan_period="FY2024",
        total_assets_usd=100_000_000.0,
        liabilities_usd=125_000_000.0,
        equity_market_value_usd=30_000_000.0,
        equity_basis_scope="equity-sleeve",
        module_version="v0.1.0",
        source_snapshot_id="snapshot:2024-06-30",
    )
    assert result.scope_label == "equity-sleeve"


def test_combined_shock_adds_discount_rate_leg() -> None:
    result = run_funded_status_combined_shock(
        plan_id="CA-TEST",
        plan_period="FY2024",
        total_assets_usd=100_000_000.0,
        liabilities_usd=125_000_000.0,
        equity_market_value_usd=40_000_000.0,
        equity_basis_scope="total-plan",
        liability_duration_years=12.0,
        equity_shock_pct=EQUITY_SHOCK,
        discount_rate_shock=-0.01,
        module_version="v0.1.0",
        source_snapshot_id="snapshot:2024-06-30",
    )
    # Δliab = -12 * (-0.01) * 125M = +15M -> liabilities rise, funded status
    # falls harder than the equity-only shock.
    assert result.liability_delta_usd == pytest.approx(15_000_000.0)
    assert result.shocked_funded_ratio < 0.736
    assert result.scope_label == "total-plan"


# ---------------------------------------------------------------------------
# Task 3 — fee / cost-effectiveness
# ---------------------------------------------------------------------------
def test_fee_effectiveness_vs_anchor_and_alts() -> None:
    report = compute_fee_effectiveness(
        plan_id="CA-TEST",
        plan_period="FY2024",
        investment_expense_usd=650_000.0,
        total_assets_usd=100_000_000.0,
        scope_label="total-plan",
        alts_management_fees_usd=[200_000.0, 100_000.0, float("nan")],
        alts_carried_interest_usd=[300_000.0],
        net_return=0.072,
        policy_benchmark_return=0.065,
        provenance_refs=["cafr:2024", "ab2833:2024"],
    )
    # 650k / 100M = 65 bps vs 40 bps anchor -> +25 bps excess
    assert report.expense_bps == pytest.approx(65.0)
    assert report.excess_bps == pytest.approx(25.0)
    # NaN component dropped, not poisoned to NaN.
    assert report.alts_management_fees_usd == pytest.approx(300_000.0)
    assert report.alts_carried_interest_usd == pytest.approx(300_000.0)
    assert report.net_value_added == pytest.approx(0.007)


# ---------------------------------------------------------------------------
# Task 5 — factor tilts (no fabrication where absent)
# ---------------------------------------------------------------------------
def test_factor_tilts_weight_average_and_missing_hook() -> None:
    weights = {"AAA": 0.5, "BBB": 0.5}
    report = compute_factor_tilts(
        plan_id="CA-TEST",
        plan_period="FY2024",
        weights=weights,
        factor_exposures={
            "AAA": {"value": 1.0, "momentum": -0.5},
            "BBB": {"value": -1.0},
        },
        provenance_refs=["factor:2024"],
    )
    # value present on both: 0.5*1.0 + 0.5*(-1.0) = 0.0
    assert report.factor_zscores["value"] == pytest.approx(0.0)
    assert report.factor_coverage["value"] == pytest.approx(1.0)
    # momentum only on AAA (covered weight 0.5) -> weighted avg over covered = -0.5
    assert report.factor_zscores["momentum"] == pytest.approx(-0.5)
    assert report.factor_coverage["momentum"] == pytest.approx(0.5)
    # quality/size absent everywhere -> None, listed missing (no fabrication).
    assert report.factor_zscores["quality"] is None
    assert report.factor_zscores["size"] is None
    assert "quality" in report.factors_missing
    assert report.scope_label == "equity-sleeve"


# ---------------------------------------------------------------------------
# Task 6 — liquidity & Brinson attribution
# ---------------------------------------------------------------------------
def test_liquidity_illiquid_pct_and_outflow_coverage() -> None:
    report = compute_liquidity(
        plan_id="CA-TEST",
        plan_period="FY2024",
        illiquid_market_value_usd=30_000_000.0,
        total_market_value_usd=100_000_000.0,
        scope_label="total-plan",
        net_cash_outflow_usd=5_000_000.0,
        provenance_refs=["cafr:2024"],
    )
    assert report.illiquid_pct == pytest.approx(0.3)
    # liquid 70M / 5M outflow = 14 years of coverage
    assert report.outflow_coverage_ratio == pytest.approx(14.0)


def test_brinson_attribution_decomposes_active_return() -> None:
    report = compute_brinson_attribution(
        plan_id="CA-TEST",
        plan_period="FY2024",
        portfolio_weights={"equity": 0.6, "bonds": 0.4},
        portfolio_returns={"equity": 0.10, "bonds": 0.03},
        policy_weights={"equity": 0.5, "bonds": 0.5},
        policy_returns={"equity": 0.08, "bonds": 0.04},
        scope_label="total-plan",
        fee_drag=0.002,
        provenance_refs=["cafr:2024"],
    )
    # Effects should sum to total active return; net-of-fee subtracts the drag.
    assert report.total_active_return == pytest.approx(
        report.allocation_effect + report.selection_effect + report.interaction_effect
    )
    assert report.total_active_return_net_of_fee == pytest.approx(
        report.total_active_return - 0.002
    )


# ---------------------------------------------------------------------------
# Finite-guard discipline (#636) — NaN/inf rejected, not silently passed
# ---------------------------------------------------------------------------
def test_finite_guards_reject_non_finite_inputs() -> None:
    with pytest.raises(ValueError, match="must be a finite number"):
        compute_allocation_drift(
            plan_id="p",
            plan_period="FY2024",
            actual_weights={"public_equity": float("nan")},
            target_weights={"public_equity": 0.5},
            scope_label="total-plan",
            provenance_refs=[],
        )
    with pytest.raises(ValueError, match="must be a finite number"):
        compute_concentration(
            plan_id="p",
            plan_period="FY2024",
            weights={"AAA": float("inf")},
        )
    with pytest.raises(ValueError, match="must be a finite number"):
        compute_active_share(
            plan_id="p",
            plan_period="FY2024",
            portfolio_weights={"AAA": float("nan")},
            benchmark_weights={"AAA": 0.5},
        )
    with pytest.raises(ValueError, match="must be a finite number"):
        run_funded_status_equity_shock(
            plan_id="p",
            plan_period="FY2024",
            total_assets_usd=float("inf"),
            liabilities_usd=125_000_000.0,
            equity_market_value_usd=40_000_000.0,
            equity_basis_scope="total-plan",
            module_version="v0.1.0",
            source_snapshot_id="snapshot",
        )


def test_positions_to_weights_drops_non_finite_market_values() -> None:
    positions = _equity_positions()
    poisoned = [
        PlanSecurityPosition(
            plan_id="CA-TEST",
            plan_period="FY2024",
            security_id="cusip:NAN",
            security_name="Bad",
            cusip="NAN",
            ticker=None,
            shares=None,
            market_value_usd=float("nan"),
            asset_class="public_equity",
            source="13f",
            as_of="2024-06-30",
            disclosure_state="disclosed",
            provenance_ref="prov:nan",
            valid_from="2024-06-30",
            asserted_at="2024-06-30",
        )
    ]
    weights = positions_to_weights(positions + poisoned)
    # The NaN-valued position is dropped; the finite universe still sums to 1.
    assert "cusip:NAN" not in weights
    assert sum(weights.values()) == pytest.approx(1.0)
    assert all(math.isfinite(w) for w in weights.values())
