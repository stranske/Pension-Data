"""Pension-Data quant economic invariants.

These are properties that must hold for the quant surfaces regardless of the
scenario, grounded in the formulas in ``scenarios.py`` and ``metric_engine.py``
-- NOT generic placeholders.

Deterministic-scenario contract (``run_deterministic_scenario`` /
``_scenario_metric_value``):
  * delta identity:        delta == scenario_value - baseline_value
  * baseline pass-through:  baseline_value equals the supplied baseline level
  * no-op metrics:          a metric with no shock/knob is unchanged (delta 0)
  * fee bps conversion:     fee_rate.value == fee_rate.baseline + fee_delta_bps/10000
  * return override:        net_return.value == return_override (when set)

Derived-metric contract (``compute_derived_metrics``):
  * funded-gap identity:    funded_gap_usd == aal_usd - ava_usd
  * unfunded-ratio identity: unfunded_ratio == funded_gap_usd / aal_usd
  * unfunded bound:          ava >= 0 and aal > 0  =>  unfunded_ratio <= 1
  * net-cash-flow identity:  net_cash_flow_usd == sum of the four normalized flows
  * coverage identity:       contribution_to_benefit_ratio == (er+ee)/abs(benefit)
  * coverage sign:           non-negative contributions => ratio >= 0

The result type and assertion helper are shared
(``baseline_kit.InvariantResult`` / ``assert_invariants``).
"""

from __future__ import annotations

from typing import Any

from baseline_kit import InvariantResult

from . import adapter

_EPS = 1e-9
_BPS = 10_000.0


def _approx(a: float, b: float, *, tol: float = 1e-6) -> bool:
    return abs(a - b) <= tol + _EPS


def _knobs(spec: dict[str, Any]) -> dict[str, Any]:
    return spec.get("scenario", {})


def _core_value(spec: dict[str, Any], metric_name: str) -> float | None:
    for row in spec.get("facts", {}).get("core_metric_rows", []):
        if row.get("metric_name") == metric_name:
            return float(row["normalized_value"])
    return None


def _cash_row(spec: dict[str, Any], cash_flow_id: str) -> dict[str, Any] | None:
    for row in spec.get("facts", {}).get("cash_flow_rows", []):
        if row.get("cash_flow_id") == cash_flow_id:
            return row
    return None


def check_scenario(scenario: dict[str, Any], base: dict[str, Any]) -> list[InvariantResult]:
    """Run every invariant against one scenario's flattened metrics."""
    spec = adapter.apply_patch(base, scenario.get("patch"))
    metrics = adapter.run_scenario(scenario, base)
    knobs = _knobs(spec)
    shocks = knobs.get("macro_shocks", {})

    results: list[InvariantResult] = []

    def add(name: str, ok: bool, detail: str, severity: str = "error") -> None:
        results.append(InvariantResult(name, bool(ok), severity, detail))

    # --- Deterministic-scenario contract --------------------------------------
    plan = str(spec.get("plan_id", "CA-PERS"))
    period = str(spec.get("plan_period", "FY2024"))
    contribution_delta = float(knobs.get("contribution_delta", 0.0))
    fee_delta_bps = float(knobs.get("fee_delta_bps", 0.0))
    return_override = knobs.get("return_override")

    for metric in adapter.DET_METRICS:
        baseline = metrics[f"det.{metric}.baseline"]
        value = metrics[f"det.{metric}.value"]
        delta = metrics[f"det.{metric}.delta"]
        declared_baseline = float(spec["baseline_metrics"][metric])

        # delta is exactly the value minus the baseline.
        add(
            f"det.{metric}.delta_identity",
            _approx(delta, value - baseline),
            f"delta={delta} value-baseline={value - baseline}",
        )
        # baseline pass-through: the reported baseline equals the supplied level.
        add(
            f"det.{metric}.baseline_passthrough",
            _approx(baseline, declared_baseline),
            f"baseline={baseline} declared={declared_baseline}",
        )

        # Determine whether any knob/shock should move this metric.
        touched = metric in shocks
        if metric in {"employer_contributions", "employee_contributions"}:
            touched = touched or contribution_delta != 0.0
        if metric == "fee_rate":
            touched = touched or fee_delta_bps != 0.0
        if metric == "net_return":
            touched = touched or return_override is not None

        if not touched:
            # A metric no knob/shock targets passes through unchanged.
            add(
                f"det.{metric}.untouched_is_noop",
                _approx(value, baseline) and _approx(delta, 0.0),
                f"value={value} baseline={baseline} delta={delta}",
            )

    # fee bps conversion: value == baseline + fee_delta_bps / 10000 (+ any shock).
    fee_baseline = metrics["det.fee_rate.baseline"]
    fee_expected = fee_baseline + fee_delta_bps / _BPS + float(shocks.get("fee_rate", 0.0))
    add(
        "det.fee_rate.bps_conversion",
        _approx(metrics["det.fee_rate.value"], fee_expected),
        f"fee={metrics['det.fee_rate.value']} expected={fee_expected}",
    )

    # return override replaces net_return entirely (when set).
    if return_override is not None:
        add(
            "det.net_return.override_replaces",
            _approx(metrics["det.net_return.value"], float(return_override)),
            f"net_return={metrics['det.net_return.value']} override={return_override}",
        )

    # --- Derived-metric contract ----------------------------------------------
    aal = _core_value(spec, "aal_usd")
    ava = _core_value(spec, "ava_usd")
    gap_key = f"derived.{plan}.{period}.funded_gap_usd"
    ratio_key = f"derived.{plan}.{period}.unfunded_ratio"
    if aal is not None and ava is not None and gap_key in metrics:
        funded_gap = metrics[gap_key]
        add(
            "derived.funded_gap_identity",
            _approx(funded_gap, aal - ava),
            f"funded_gap={funded_gap} aal-ava={aal - ava}",
        )
        if aal != 0 and ratio_key in metrics:
            unfunded = metrics[ratio_key]
            add(
                "derived.unfunded_ratio_identity",
                _approx(unfunded, funded_gap / aal),
                f"unfunded={unfunded} gap/aal={funded_gap / aal}",
            )
            if ava >= 0 and aal > 0:
                # Non-negative assets against positive liability: the unfunded
                # share cannot exceed 100%.
                add(
                    "derived.unfunded_ratio_le_one",
                    unfunded <= 1.0 + _EPS,
                    f"unfunded={unfunded}",
                )

    cash = _cash_row(spec, "flow:2024")
    ncf_key = f"derived.{plan}.{period}.net_cash_flow_usd"
    cov_key = f"derived.{plan}.{period}.contribution_to_benefit_ratio"
    if cash is not None and ncf_key in metrics:
        employer = float(cash["employer_contributions_normalized"])
        employee = float(cash["employee_contributions_normalized"])
        benefit = float(cash["benefit_payments_normalized"])
        refunds = float(cash["refunds_normalized"])
        add(
            "derived.net_cash_flow_identity",
            _approx(metrics[ncf_key], employer + employee + benefit + refunds),
            f"ncf={metrics[ncf_key]} sum={employer + employee + benefit + refunds}",
        )
        if benefit != 0 and cov_key in metrics:
            coverage = metrics[cov_key]
            add(
                "derived.coverage_ratio_identity",
                _approx(coverage, (employer + employee) / abs(benefit)),
                f"coverage={coverage} expected={(employer + employee) / abs(benefit)}",
            )
            if employer >= 0 and employee >= 0:
                # Non-negative contributions over a positive denominator: the
                # coverage ratio is non-negative.
                add(
                    "derived.coverage_ratio_non_negative",
                    coverage >= -_EPS,
                    f"coverage={coverage}",
                )

    return results
