"""Holdings analytics layer (issue #648).

Turns collected security-level holdings (#647) into decision-useful portfolio
analysis for a single plan-period: allocation-vs-policy drift, funded-status
stress, fee/cost-effectiveness, manager overlap / active share, look-through
concentration & factor tilts, and (where data allows) liquidity & Brinson
attribution.

Design contract — the #1 rule (R3 guardrail)
--------------------------------------------
Every analytic output carries an explicit :data:`ScopeLabel`:

* ``"total-plan"`` — CAFR/ACFR-anchored, covers the whole plan.
* ``"equity-sleeve"`` — 13F-only public-equity slice (~25-46% of a typical plan).

A 13F-equity-sleeve number is **never** presented as total-plan. Total-plan
figures must be anchored to the CAFR/ACFR coverage report
(:class:`~pension_data.db.models.investment_positions.HoldingsCoverageReport`
from #647). Concentration, active share, and factor tilts are intrinsically
equity-sleeve; drift, funded-status, and fees are total-plan only when their
inputs are CAFR-anchored, and are otherwise downgraded to equity-sleeve.

Reuse (do not reimplement)
--------------------------
* Funded-status stress routes through
  :func:`pension_data.quant.scenarios.run_deterministic_scenario`.
* The peer leg of allocation-vs-policy reuses
  :func:`pension_data.query.saved_views.service.execute_allocation_peer_compare_view`.
* Security-level overlap extends the ``holdings_overlap`` min-exposure seam.
* Every numeric boundary is guarded by :mod:`pension_data.finite_guards`.
* Provenance is normalized via
  :func:`pension_data.quant.contracts.normalize_provenance_refs`.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal

from pension_data.db.models.investment_positions import (
    HoldingsCoverageReport,
    PlanSecurityPosition,
)
from pension_data.finite_guards import is_finite_number, require_finite
from pension_data.quant.contracts import normalize_provenance_refs
from pension_data.quant.scenarios import (
    ScenarioInput,
    ScenarioResult,
    ScenarioRunConfig,
    run_deterministic_scenario,
)
from pension_data.query.saved_views.models import AllocationPeerInput, AllocationPeerRow
from pension_data.query.saved_views.service import execute_allocation_peer_compare_view

__all__ = [
    "ScopeLabel",
    "CremersBand",
    "AllocationDriftRow",
    "AllocationDriftReport",
    "FundedStatusStressResult",
    "FeeEffectivenessReport",
    "SecurityOverlapRow",
    "ActiveShareReport",
    "ConcentrationReport",
    "FactorTiltReport",
    "LiquidityReport",
    "BrinsonAttributionReport",
    "DEFAULT_ALTERNATIVES",
    "DEFAULT_FEE_ANCHOR_BPS",
    "coverage_scope_label",
    "positions_to_weights",
    "compute_allocation_drift",
    "run_funded_status_equity_shock",
    "run_funded_status_combined_shock",
    "compute_fee_effectiveness",
    "compute_security_overlap",
    "compute_active_share",
    "compute_concentration",
    "compute_factor_tilts",
    "compute_liquidity",
    "compute_brinson_attribution",
]

ScopeLabel = Literal["total-plan", "equity-sleeve"]
CremersBand = Literal[
    "closet-indexer",
    "moderately-active",
    "highly-active",
    "very-highly-active",
]

DEFAULT_FEE_ANCHOR_BPS = 40.0
"""Cost-effectiveness anchor: a passive-ish ~40 bps all-in investment expense."""

DEFAULT_ALTERNATIVES: frozenset[str] = frozenset(
    {
        "alternatives",
        "commodities",
        "hedge_funds",
        "infrastructure",
        "opportunistic",
        "private_credit",
        "private_debt",
        "private_equity",
        "real_assets",
        "real_estate",
    }
)
"""Asset-class names treated as alternatives for over/underweight rollup."""


# ---------------------------------------------------------------------------
# Scope / weight helpers (shared plumbing)
# ---------------------------------------------------------------------------
def coverage_scope_label(coverage: HoldingsCoverageReport) -> ScopeLabel:
    """Return the CAFR-anchored scope label from a #647 coverage report.

    Reuses the coverage report's own threshold decision so the analytics layer
    and the ingestion layer agree byte-for-byte on what counts as total-plan.
    """
    return "total-plan" if coverage.scope_label == "total-plan" else "equity-sleeve"


def _normalized_class(name: str) -> str:
    return "_".join(name.strip().lower().split())


def positions_to_weights(
    positions: Iterable[PlanSecurityPosition],
) -> dict[str, float]:
    """Collapse disclosed security positions into ``security_id -> weight``.

    Only ``disclosed`` positions with a finite, positive market value contribute.
    Weights are normalized to sum to 1 over the disclosed, finite universe. An
    empty or all-non-finite universe yields an empty mapping (no fabrication).
    """
    market_values: dict[str, float] = {}
    for position in positions:
        if not position.is_disclosed:
            continue
        value = position.market_value_usd
        if not is_finite_number(value) or value is None or value <= 0.0:
            continue
        market_values[position.security_id] = market_values.get(position.security_id, 0.0) + float(
            value
        )
    total = sum(market_values.values())
    if total <= 0.0 or not is_finite_number(total):
        return {}
    return {security_id: value / total for security_id, value in market_values.items()}


def _weighted_gross_abs(deltas: Iterable[float]) -> float:
    return sum(abs(delta) for delta in deltas)


# ---------------------------------------------------------------------------
# Task 1 — Allocation vs policy & peers
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class AllocationDriftRow:
    """Per-asset-class active weight (actual minus policy target)."""

    asset_class: str
    actual_weight: float
    target_weight: float
    active_weight: float
    is_alternative: bool


@dataclass(frozen=True, slots=True)
class AllocationDriftReport:
    """Allocation-vs-policy drift with optional peer positioning."""

    plan_id: str
    plan_period: str
    scope_label: ScopeLabel
    drift: float
    rows: tuple[AllocationDriftRow, ...]
    alternatives_active_weight: float
    peer_rows: tuple[AllocationPeerRow, ...]
    provenance_refs: tuple[str, ...]


def compute_allocation_drift(
    *,
    plan_id: str,
    plan_period: str,
    actual_weights: Mapping[str, float],
    target_weights: Mapping[str, float],
    scope_label: ScopeLabel,
    provenance_refs: Sequence[str],
    alternatives: Iterable[str] = DEFAULT_ALTERNATIVES,
    peer_rows: Sequence[AllocationPeerInput] = (),
    alternatives_normalizer: bool = True,
) -> AllocationDriftReport:
    """Compute allocation drift ``= ½·Σ|actualᵢ − targetᵢ|`` across asset classes.

    ``actual_weights`` and ``target_weights`` are asset-class → weight fractions.
    Missing classes on either side are treated as a zero weight, so a class held
    but not targeted (or targeted but not held) still contributes to drift. The
    peer leg reuses
    :func:`~pension_data.query.saved_views.service.execute_allocation_peer_compare_view`.

    ``scope_label`` is the caller's responsibility: pass ``"total-plan"`` only when
    the weights are CAFR-anchored asset-allocation percentages.
    """
    alt_set = (
        {_normalized_class(name) for name in alternatives}
        if alternatives_normalizer
        else set(alternatives)
    )
    classes = sorted(
        {_normalized_class(name) for name in actual_weights}
        | {_normalized_class(name) for name in target_weights}
    )
    actual_norm = {
        _normalized_class(k): require_finite(v, field=f"actual_weights[{k!r}]")
        for k, v in actual_weights.items()
    }
    target_norm = {
        _normalized_class(k): require_finite(v, field=f"target_weights[{k!r}]")
        for k, v in target_weights.items()
    }

    rows: list[AllocationDriftRow] = []
    for asset_class in classes:
        actual = actual_norm.get(asset_class, 0.0)
        target = target_norm.get(asset_class, 0.0)
        active = actual - target
        rows.append(
            AllocationDriftRow(
                asset_class=asset_class,
                actual_weight=round(actual, 6),
                target_weight=round(target, 6),
                active_weight=round(active, 6),
                is_alternative=asset_class in alt_set,
            )
        )

    drift = round(0.5 * _weighted_gross_abs(row.active_weight for row in rows), 6)
    alternatives_active = round(sum(row.active_weight for row in rows if row.is_alternative), 6)

    peer_out: tuple[AllocationPeerRow, ...] = ()
    if peer_rows:
        peer_out = tuple(
            execute_allocation_peer_compare_view(
                list(peer_rows), subject_plan_id=plan_id, plan_period=plan_period
            )
        )

    return AllocationDriftReport(
        plan_id=plan_id,
        plan_period=plan_period,
        scope_label=scope_label,
        drift=drift,
        rows=tuple(rows),
        alternatives_active_weight=alternatives_active,
        peer_rows=peer_out,
        provenance_refs=normalize_provenance_refs(provenance_refs),
    )


# ---------------------------------------------------------------------------
# Task 2 — Funded-status stress (routed through quant/scenarios.py)
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class FundedStatusStressResult:
    """Funded-status impact of an equity (and optional discount-rate) shock.

    ``scope_label`` is ``"total-plan"`` only when the equity market value used to
    size the shock is itself CAFR total-plan equity. A 13F-equity-sleeve equity
    basis under-sizes the shock, so the result is downgraded to ``"equity-sleeve"``
    (a partial/floor impact), never presented as total-plan.
    """

    plan_id: str
    plan_period: str
    scope_label: ScopeLabel
    equity_shock_pct: float
    discount_rate_shock: float | None
    baseline_funded_ratio: float
    shocked_funded_ratio: float
    funded_ratio_delta: float
    asset_delta_usd: float
    liability_delta_usd: float
    baseline_uaal_usd: float
    shocked_uaal_usd: float
    equity_basis_scope: ScopeLabel
    scenario: ScenarioResult
    provenance_refs: tuple[str, ...]


def _funded_status_stress(
    *,
    plan_id: str,
    plan_period: str,
    total_assets_usd: float,
    liabilities_usd: float,
    equity_market_value_usd: float,
    equity_basis_scope: ScopeLabel,
    equity_shock_pct: float,
    discount_rate_shock: float | None,
    liability_duration_years: float | None,
    module_version: str,
    source_snapshot_id: str,
    provenance_refs: Sequence[str],
    scenario_name: str,
) -> FundedStatusStressResult:
    total_assets_usd = require_finite(total_assets_usd, field="total_assets_usd")
    liabilities_usd = require_finite(liabilities_usd, field="liabilities_usd")
    equity_market_value_usd = require_finite(
        equity_market_value_usd, field="equity_market_value_usd"
    )
    equity_shock_pct = require_finite(equity_shock_pct, field="equity_shock_pct")
    if liabilities_usd <= 0.0:
        raise ValueError("liabilities_usd must be positive to define a funded ratio")

    asset_delta = equity_market_value_usd * equity_shock_pct

    liability_delta = 0.0
    if discount_rate_shock is not None:
        discount_rate_shock = require_finite(discount_rate_shock, field="discount_rate_shock")
        if liability_duration_years is None:
            raise ValueError("liability_duration_years is required when discount_rate_shock is set")
        liability_duration_years = require_finite(
            liability_duration_years, field="liability_duration_years"
        )
        if liability_duration_years < 0.0:
            raise ValueError("liability_duration_years must be non-negative")
        # Duration approximation: a fall in the discount rate (negative shock)
        # raises the present value of liabilities.
        liability_delta = -liability_duration_years * discount_rate_shock * liabilities_usd

    # Route the asset & liability shocks through the shared scenario engine so
    # the funded-status stress reuses the same deterministic machinery, config
    # hashing, and reproducibility metadata as every other quant scenario.
    macro_shocks: dict[str, float] = {"total_assets_usd": asset_delta}
    if discount_rate_shock is not None:
        macro_shocks["liabilities_usd"] = liability_delta
    scenario = run_deterministic_scenario(
        plan_id=plan_id,
        plan_period=plan_period,
        baseline_metrics={
            "total_assets_usd": total_assets_usd,
            "liabilities_usd": liabilities_usd,
        },
        scenario=ScenarioInput(name=scenario_name, macro_shocks=macro_shocks),
        config=ScenarioRunConfig(module_version=module_version),
        source_snapshot_id=source_snapshot_id,
    )
    row_map = {row.metric_name: row for row in scenario.rows}
    shocked_assets = row_map["total_assets_usd"].scenario_value
    shocked_liabilities = row_map["liabilities_usd"].scenario_value
    if shocked_liabilities <= 0.0:
        raise ValueError("shocked liabilities collapsed to non-positive; shock is unphysical")

    baseline_funded = total_assets_usd / liabilities_usd
    shocked_funded = shocked_assets / shocked_liabilities

    # A total-plan funded-status result requires a total-plan equity basis; a
    # 13F sleeve under-sizes the asset shock, so downgrade the label.
    scope_label: ScopeLabel = (
        "total-plan" if equity_basis_scope == "total-plan" else "equity-sleeve"
    )

    return FundedStatusStressResult(
        plan_id=plan_id,
        plan_period=plan_period,
        scope_label=scope_label,
        equity_shock_pct=equity_shock_pct,
        discount_rate_shock=discount_rate_shock,
        baseline_funded_ratio=round(baseline_funded, 6),
        shocked_funded_ratio=round(shocked_funded, 6),
        funded_ratio_delta=round(shocked_funded - baseline_funded, 6),
        asset_delta_usd=round(asset_delta, 6),
        liability_delta_usd=round(liability_delta, 6),
        baseline_uaal_usd=round(liabilities_usd - total_assets_usd, 6),
        shocked_uaal_usd=round(shocked_liabilities - shocked_assets, 6),
        equity_basis_scope=equity_basis_scope,
        scenario=scenario,
        provenance_refs=normalize_provenance_refs(provenance_refs),
    )


def run_funded_status_equity_shock(
    *,
    plan_id: str,
    plan_period: str,
    total_assets_usd: float,
    liabilities_usd: float,
    equity_market_value_usd: float,
    equity_basis_scope: ScopeLabel,
    equity_shock_pct: float = -0.20,
    module_version: str,
    source_snapshot_id: str,
    provenance_refs: Sequence[str] = (),
) -> FundedStatusStressResult:
    """Funded-status impact of an equity-only shock (default −20%), via scenarios.py."""
    return _funded_status_stress(
        plan_id=plan_id,
        plan_period=plan_period,
        total_assets_usd=total_assets_usd,
        liabilities_usd=liabilities_usd,
        equity_market_value_usd=equity_market_value_usd,
        equity_basis_scope=equity_basis_scope,
        equity_shock_pct=equity_shock_pct,
        discount_rate_shock=None,
        liability_duration_years=None,
        module_version=module_version,
        source_snapshot_id=source_snapshot_id,
        provenance_refs=provenance_refs,
        scenario_name=f"equity-shock:{equity_shock_pct:+.2f}",
    )


def run_funded_status_combined_shock(
    *,
    plan_id: str,
    plan_period: str,
    total_assets_usd: float,
    liabilities_usd: float,
    equity_market_value_usd: float,
    equity_basis_scope: ScopeLabel,
    liability_duration_years: float,
    equity_shock_pct: float = -0.20,
    discount_rate_shock: float = -0.01,
    module_version: str,
    source_snapshot_id: str,
    provenance_refs: Sequence[str] = (),
) -> FundedStatusStressResult:
    """Combined equity + discount-rate shock on funded status, via scenarios.py.

    ``discount_rate_shock`` is the additive change in the discount rate (e.g.
    ``-0.01`` for a 100 bps fall). Liability sensitivity uses the standard
    duration approximation ``Δliab ≈ −duration × Δr × liab``.
    """
    return _funded_status_stress(
        plan_id=plan_id,
        plan_period=plan_period,
        total_assets_usd=total_assets_usd,
        liabilities_usd=liabilities_usd,
        equity_market_value_usd=equity_market_value_usd,
        equity_basis_scope=equity_basis_scope,
        equity_shock_pct=equity_shock_pct,
        discount_rate_shock=discount_rate_shock,
        liability_duration_years=liability_duration_years,
        module_version=module_version,
        source_snapshot_id=source_snapshot_id,
        provenance_refs=provenance_refs,
        scenario_name=(f"combined-shock:eq{equity_shock_pct:+.2f}:dr{discount_rate_shock:+.4f}"),
    )


# ---------------------------------------------------------------------------
# Task 3 — Fee / cost-effectiveness
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class FeeEffectivenessReport:
    """Investment-expense cost-effectiveness vs a passive anchor."""

    plan_id: str
    plan_period: str
    scope_label: ScopeLabel
    expense_bps: float
    anchor_bps: float
    excess_bps: float
    investment_expense_usd: float
    alts_management_fees_usd: float | None
    alts_carried_interest_usd: float | None
    net_value_added: float | None
    provenance_refs: tuple[str, ...]


def _sum_finite_optional(values: Iterable[float | None]) -> float | None:
    present = [float(v) for v in values if is_finite_number(v)]
    return round(sum(present), 6) if present else None


def compute_fee_effectiveness(
    *,
    plan_id: str,
    plan_period: str,
    investment_expense_usd: float,
    total_assets_usd: float,
    scope_label: ScopeLabel,
    anchor_bps: float = DEFAULT_FEE_ANCHOR_BPS,
    alts_management_fees_usd: Iterable[float | None] = (),
    alts_carried_interest_usd: Iterable[float | None] = (),
    net_return: float | None = None,
    policy_benchmark_return: float | None = None,
    provenance_refs: Sequence[str] = (),
) -> FeeEffectivenessReport:
    """Investment expense (bps of assets) vs a ~40 bps anchor, plus alts fees.

    ``alts_management_fees_usd`` / ``alts_carried_interest_usd`` are the per-fund
    components from the #647 AB 2833 capture (finite components only; non-finite
    dropped, never poisoning the total to NaN). ``net_value_added`` is
    ``net_return − policy_benchmark_return`` when both are available.
    """
    investment_expense_usd = require_finite(investment_expense_usd, field="investment_expense_usd")
    total_assets_usd = require_finite(total_assets_usd, field="total_assets_usd")
    anchor_bps = require_finite(anchor_bps, field="anchor_bps")
    if total_assets_usd <= 0.0:
        raise ValueError("total_assets_usd must be positive to express a fee ratio")

    expense_bps = round(investment_expense_usd / total_assets_usd * 10_000.0, 4)
    net_value_added: float | None = None
    if is_finite_number(net_return) and is_finite_number(policy_benchmark_return):
        assert net_return is not None and policy_benchmark_return is not None
        net_value_added = round(net_return - policy_benchmark_return, 6)

    return FeeEffectivenessReport(
        plan_id=plan_id,
        plan_period=plan_period,
        scope_label=scope_label,
        expense_bps=expense_bps,
        anchor_bps=round(anchor_bps, 4),
        excess_bps=round(expense_bps - anchor_bps, 4),
        investment_expense_usd=round(investment_expense_usd, 6),
        alts_management_fees_usd=_sum_finite_optional(alts_management_fees_usd),
        alts_carried_interest_usd=_sum_finite_optional(alts_carried_interest_usd),
        net_value_added=net_value_added,
        provenance_refs=normalize_provenance_refs(provenance_refs),
    )


# ---------------------------------------------------------------------------
# Task 4 — Manager overlap / active share (equity sleeve)
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class SecurityOverlapRow:
    """One security's overlap between subject and counterparty equity sleeves."""

    security_id: str
    subject_weight: float
    counterparty_weight: float
    overlap_weight: float


@dataclass(frozen=True, slots=True)
class ActiveShareReport:
    """Active share vs a benchmark plus pairwise overlap, on the equity sleeve.

    Active share and overlap are intrinsically 13F-equity-sleeve figures, so the
    scope label is fixed to ``"equity-sleeve"``.
    """

    plan_id: str
    plan_period: str
    scope_label: ScopeLabel
    active_share: float
    active_share_pct: float
    cremers_band: CremersBand
    pairwise_overlap: float
    fee_per_unit_active_risk: float | None
    overlap_rows: tuple[SecurityOverlapRow, ...]
    provenance_refs: tuple[str, ...]


def _cremers_band(active_share_pct: float) -> CremersBand:
    if active_share_pct < 20.0:
        return "closet-indexer"
    if active_share_pct <= 60.0:
        return "moderately-active"
    if active_share_pct <= 80.0:
        return "highly-active"
    return "very-highly-active"


def compute_security_overlap(
    *,
    subject_weights: Mapping[str, float],
    counterparty_weights: Mapping[str, float],
) -> tuple[tuple[SecurityOverlapRow, ...], float]:
    """Security-level overlap = ``Σ min(w_subject,i, w_counterparty,i)``.

    Extends the ``holdings_overlap`` min-exposure seam from manager/fund level to
    the security level. Returns per-security rows and the scalar pairwise overlap.
    """
    all_ids = sorted(set(subject_weights) | set(counterparty_weights))
    rows: list[SecurityOverlapRow] = []
    overlap_total = 0.0
    for security_id in all_ids:
        subject = require_finite(
            subject_weights.get(security_id, 0.0), field=f"subject_weights[{security_id!r}]"
        )
        counterparty = require_finite(
            counterparty_weights.get(security_id, 0.0),
            field=f"counterparty_weights[{security_id!r}]",
        )
        overlap = min(subject, counterparty)
        overlap_total += overlap
        rows.append(
            SecurityOverlapRow(
                security_id=security_id,
                subject_weight=round(subject, 6),
                counterparty_weight=round(counterparty, 6),
                overlap_weight=round(overlap, 6),
            )
        )
    return tuple(rows), round(overlap_total, 6)


def compute_active_share(
    *,
    plan_id: str,
    plan_period: str,
    portfolio_weights: Mapping[str, float],
    benchmark_weights: Mapping[str, float],
    fee_bps: float | None = None,
    counterparty_weights: Mapping[str, float] | None = None,
    provenance_refs: Sequence[str] = (),
) -> ActiveShareReport:
    """Active share ``= ½·Σ|w_portfolio,i − w_benchmark,i|`` with Cremers-Petajisto bands.

    Active share is a fraction in ``[0, 1]``; ``active_share_pct`` is the 0-100
    form used for the bands (<20 closet, 20-60 moderate, 60-80 high, >80 very
    high). ``fee_per_unit_active_risk`` = ``fee_bps / active_share_pct`` (bps of
    fee per point of active share), ``None`` when active share is zero.
    ``counterparty_weights`` optionally drives the pairwise-overlap leg.
    """
    all_ids = sorted(set(portfolio_weights) | set(benchmark_weights))
    gross = 0.0
    for security_id in all_ids:
        w_p = require_finite(
            portfolio_weights.get(security_id, 0.0),
            field=f"portfolio_weights[{security_id!r}]",
        )
        w_b = require_finite(
            benchmark_weights.get(security_id, 0.0),
            field=f"benchmark_weights[{security_id!r}]",
        )
        gross += abs(w_p - w_b)
    active_share = round(0.5 * gross, 6)
    active_share_pct = round(active_share * 100.0, 4)

    overlap_rows: tuple[SecurityOverlapRow, ...] = ()
    pairwise_overlap = 0.0
    if counterparty_weights is not None:
        overlap_rows, pairwise_overlap = compute_security_overlap(
            subject_weights=portfolio_weights,
            counterparty_weights=counterparty_weights,
        )

    fee_per_unit: float | None = None
    if fee_bps is not None:
        fee_bps = require_finite(fee_bps, field="fee_bps")
        if active_share_pct > 0.0:
            fee_per_unit = round(fee_bps / active_share_pct, 6)

    return ActiveShareReport(
        plan_id=plan_id,
        plan_period=plan_period,
        scope_label="equity-sleeve",
        active_share=active_share,
        active_share_pct=active_share_pct,
        cremers_band=_cremers_band(active_share_pct),
        pairwise_overlap=pairwise_overlap,
        fee_per_unit_active_risk=fee_per_unit,
        overlap_rows=overlap_rows,
        provenance_refs=normalize_provenance_refs(provenance_refs),
    )


# ---------------------------------------------------------------------------
# Task 5 — Look-through concentration & factor tilts (equity sleeve)
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class ConcentrationReport:
    """Herfindahl-Hirschman concentration and top-N weight of the equity sleeve."""

    plan_id: str
    plan_period: str
    scope_label: ScopeLabel
    hhi: float
    effective_n: float | None
    top_n: int
    top_n_weight: float
    n_holdings: int
    provenance_refs: tuple[str, ...]


def compute_concentration(
    *,
    plan_id: str,
    plan_period: str,
    weights: Mapping[str, float],
    top_n: int = 10,
    provenance_refs: Sequence[str] = (),
) -> ConcentrationReport:
    """HHI ``= Σ wᵢ²``, effective holdings ``1/HHI``, and top-N weight.

    Operates on the 13F equity sleeve, so the scope label is fixed to
    ``"equity-sleeve"``. Weights are expected to be a normalized fraction vector.
    """
    if top_n <= 0:
        raise ValueError("top_n must be positive")
    finite_weights = {
        security_id: require_finite(weight, field=f"weights[{security_id!r}]")
        for security_id, weight in weights.items()
    }
    hhi = round(sum(weight * weight for weight in finite_weights.values()), 6)
    effective_n = round(1.0 / hhi, 6) if hhi > 0.0 else None
    ordered = sorted(finite_weights.values(), reverse=True)
    top_n_weight = round(sum(ordered[:top_n]), 6)
    return ConcentrationReport(
        plan_id=plan_id,
        plan_period=plan_period,
        scope_label="equity-sleeve",
        hhi=hhi,
        effective_n=effective_n,
        top_n=top_n,
        top_n_weight=top_n_weight,
        n_holdings=len(finite_weights),
        provenance_refs=normalize_provenance_refs(provenance_refs),
    )


@dataclass(frozen=True, slots=True)
class FactorTiltReport:
    """Weighted factor z-scores over the equity sleeve (no fabrication).

    ``factor_zscores`` maps each requested factor to the sleeve's weight-average
    standardized exposure, or ``None`` when no held security carries that factor.
    ``factor_coverage`` reports the weight fraction of the sleeve that carried an
    exposure for each factor, so a thinly-covered tilt is never mistaken for a
    fully-informed one.
    """

    plan_id: str
    plan_period: str
    scope_label: ScopeLabel
    factor_zscores: dict[str, float | None]
    factor_coverage: dict[str, float]
    factors_missing: tuple[str, ...]
    provenance_refs: tuple[str, ...]


def compute_factor_tilts(
    *,
    plan_id: str,
    plan_period: str,
    weights: Mapping[str, float],
    factor_exposures: Mapping[str, Mapping[str, float]],
    factors: Sequence[str] = ("value", "quality", "size", "momentum"),
    provenance_refs: Sequence[str] = (),
) -> FactorTiltReport:
    """Weight-average standardized factor exposures over the equity sleeve.

    ``factor_exposures`` maps ``security_id -> {factor -> standardized z}``. For
    each factor the report is the weight-average z over the *held securities that
    carry that factor*, with weights renormalized over the covered subset. Where a
    factor is absent for every held security the value is ``None`` and the factor
    is listed in ``factors_missing`` — the hook is exposed but no data is
    fabricated. Scope is fixed to ``"equity-sleeve"``.
    """
    finite_weights = {
        security_id: require_finite(weight, field=f"weights[{security_id!r}]")
        for security_id, weight in weights.items()
    }
    zscores: dict[str, float | None] = {}
    coverage: dict[str, float] = {}
    missing: list[str] = []
    for factor in factors:
        covered_weight = 0.0
        weighted_sum = 0.0
        for security_id, weight in finite_weights.items():
            exposures = factor_exposures.get(security_id)
            if not exposures:
                continue
            raw = exposures.get(factor)
            if not is_finite_number(raw):
                continue
            covered_weight += weight
            weighted_sum += weight * float(raw)
        if covered_weight > 0.0:
            zscores[factor] = round(weighted_sum / covered_weight, 6)
            coverage[factor] = round(covered_weight, 6)
        else:
            zscores[factor] = None
            coverage[factor] = 0.0
            missing.append(factor)
    return FactorTiltReport(
        plan_id=plan_id,
        plan_period=plan_period,
        scope_label="equity-sleeve",
        factor_zscores=zscores,
        factor_coverage=coverage,
        factors_missing=tuple(missing),
        provenance_refs=normalize_provenance_refs(provenance_refs),
    )


# ---------------------------------------------------------------------------
# Task 6 — Liquidity & Brinson attribution (where data allows)
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class LiquidityReport:
    """Illiquid share vs a net cash outflow (liquidity-adequacy view)."""

    plan_id: str
    plan_period: str
    scope_label: ScopeLabel
    illiquid_market_value_usd: float
    total_market_value_usd: float
    illiquid_pct: float
    net_cash_outflow_usd: float | None
    liquid_market_value_usd: float
    outflow_coverage_ratio: float | None
    provenance_refs: tuple[str, ...]


def compute_liquidity(
    *,
    plan_id: str,
    plan_period: str,
    illiquid_market_value_usd: float,
    total_market_value_usd: float,
    scope_label: ScopeLabel,
    net_cash_outflow_usd: float | None = None,
    provenance_refs: Sequence[str] = (),
) -> LiquidityReport:
    """Illiquid share of the portfolio and its coverage of a net cash outflow.

    ``outflow_coverage_ratio`` = ``liquid_market_value / net_cash_outflow`` (how
    many years of outflow the liquid book covers), ``None`` when outflow is not
    supplied or non-positive.
    """
    illiquid_market_value_usd = require_finite(
        illiquid_market_value_usd, field="illiquid_market_value_usd"
    )
    total_market_value_usd = require_finite(total_market_value_usd, field="total_market_value_usd")
    if total_market_value_usd <= 0.0:
        raise ValueError("total_market_value_usd must be positive")
    if illiquid_market_value_usd < 0.0:
        raise ValueError("illiquid_market_value_usd must be non-negative")
    liquid = total_market_value_usd - illiquid_market_value_usd
    outflow_coverage: float | None = None
    outflow_clean: float | None = None
    if net_cash_outflow_usd is not None:
        outflow_clean = require_finite(net_cash_outflow_usd, field="net_cash_outflow_usd")
        if outflow_clean > 0.0:
            outflow_coverage = round(liquid / outflow_clean, 6)
    return LiquidityReport(
        plan_id=plan_id,
        plan_period=plan_period,
        scope_label=scope_label,
        illiquid_market_value_usd=round(illiquid_market_value_usd, 6),
        total_market_value_usd=round(total_market_value_usd, 6),
        illiquid_pct=round(illiquid_market_value_usd / total_market_value_usd, 6),
        net_cash_outflow_usd=None if outflow_clean is None else round(outflow_clean, 6),
        liquid_market_value_usd=round(liquid, 6),
        outflow_coverage_ratio=outflow_coverage,
        provenance_refs=normalize_provenance_refs(provenance_refs),
    )


@dataclass(frozen=True, slots=True)
class BrinsonAttributionReport:
    """Brinson allocation/selection attribution vs a policy benchmark."""

    plan_id: str
    plan_period: str
    scope_label: ScopeLabel
    allocation_effect: float
    selection_effect: float
    interaction_effect: float
    total_active_return: float
    total_active_return_net_of_fee: float
    fee_drag: float
    per_bucket: dict[str, dict[str, float]] = field(default_factory=dict)
    provenance_refs: tuple[str, ...] = ()


def compute_brinson_attribution(
    *,
    plan_id: str,
    plan_period: str,
    portfolio_weights: Mapping[str, float],
    portfolio_returns: Mapping[str, float],
    policy_weights: Mapping[str, float],
    policy_returns: Mapping[str, float],
    scope_label: ScopeLabel,
    fee_drag: float = 0.0,
    provenance_refs: Sequence[str] = (),
) -> BrinsonAttributionReport:
    """Brinson-Fachler allocation/selection/interaction attribution vs policy.

    Per bucket ``i`` with policy weight ``Wᵢ``/return ``Bᵢ`` and portfolio weight
    ``wᵢ``/return ``Rᵢ`` against total benchmark return ``B̄``:

    * allocation ``= (wᵢ − Wᵢ)·(Bᵢ − B̄)``
    * selection  ``= Wᵢ·(Rᵢ − Bᵢ)``
    * interaction ``= (wᵢ − Wᵢ)·(Rᵢ − Bᵢ)``

    ``fee_drag`` (a positive return give-up) is subtracted to report a
    net-of-fee active return. Every weight and return is finite-guarded.
    """
    buckets = sorted(set(portfolio_weights) | set(policy_weights))
    fee_drag = require_finite(fee_drag, field="fee_drag")

    def _get(mapping: Mapping[str, float], key: str, name: str) -> float:
        return require_finite(mapping.get(key, 0.0), field=f"{name}[{key!r}]")

    total_benchmark_return = sum(
        _get(policy_weights, bucket, "policy_weights")
        * _get(policy_returns, bucket, "policy_returns")
        for bucket in buckets
    )

    allocation_total = 0.0
    selection_total = 0.0
    interaction_total = 0.0
    per_bucket: dict[str, dict[str, float]] = {}
    for bucket in buckets:
        w_p = _get(portfolio_weights, bucket, "portfolio_weights")
        w_b = _get(policy_weights, bucket, "policy_weights")
        r_p = _get(portfolio_returns, bucket, "portfolio_returns")
        r_b = _get(policy_returns, bucket, "policy_returns")
        allocation = (w_p - w_b) * (r_b - total_benchmark_return)
        selection = w_b * (r_p - r_b)
        interaction = (w_p - w_b) * (r_p - r_b)
        allocation_total += allocation
        selection_total += selection
        interaction_total += interaction
        per_bucket[bucket] = {
            "allocation": round(allocation, 6),
            "selection": round(selection, 6),
            "interaction": round(interaction, 6),
        }

    total_active = allocation_total + selection_total + interaction_total
    return BrinsonAttributionReport(
        plan_id=plan_id,
        plan_period=plan_period,
        scope_label=scope_label,
        allocation_effect=round(allocation_total, 6),
        selection_effect=round(selection_total, 6),
        interaction_effect=round(interaction_total, 6),
        total_active_return=round(total_active, 6),
        total_active_return_net_of_fee=round(total_active - fee_drag, 6),
        fee_drag=round(fee_drag, 6),
        per_bucket=per_bucket,
        provenance_refs=normalize_provenance_refs(provenance_refs),
    )
