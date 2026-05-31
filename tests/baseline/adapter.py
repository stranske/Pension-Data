"""App-specific adapter for the Pension-Data quant surfaces.

This is the ONLY app-specific piece the shared ``baseline_kit`` needs: a way to
turn an input (here, a base fixture plus a scenario *patch*) into a flat dict of
named scalar metrics. Everything else -- directional checks, invariants, golden
masters, the coverage manifest -- is generic and lives in ``baseline_kit``.

Two deterministic compute surfaces (no DB, no network, no LLM) are exercised:

  * ``run_deterministic_scenario`` (scenarios.py) -- applies scenario knobs to a
    baseline-metrics dict.
  * ``compute_derived_metrics`` (metric_engine.py) -- reduces staged plan facts
    to derived economic metrics.

Scenario model
--------------
The base fixture lives in ``catalog.yaml`` under ``base`` and has three parts:

* ``baseline_metrics`` -- the named metric levels fed to
  ``run_deterministic_scenario`` (``funded_ratio``, ``employer_contributions``,
  ``employee_contributions``, ``fee_rate``, ``net_return``).
* ``scenario`` -- the scenario knobs (``macro_shocks``, ``contribution_delta``,
  ``fee_delta_bps``, ``return_override``). ``name`` defaults to the scenario id.
* ``facts`` -- the ``core_metric_rows`` (AAL/AVA) and ``cash_flow_rows`` fed to
  ``compute_derived_metrics``.

Each *scenario* is the base fixture with an optional ``patch`` applied. A patch
is an ordered list of operations -- the small DSL ``apply_patch`` understands:

* ``{op: set_baseline, metric: M, value: V}`` -- overwrite a baseline metric.
* ``{op: set_shock, metric: M, value: V}`` -- set a macro shock on a metric.
* ``{op: set_knob, knob: K, value: V}`` -- set ``contribution_delta``,
  ``fee_delta_bps`` or ``return_override`` on the scenario block.
* ``{op: set_fact, kind: core|cash, key: K, field: F, value: V}`` -- overwrite a
  field on the core/cash fact row keyed by ``key`` (``metric_name`` for core,
  ``cash_flow_id`` for cash).

This keeps the catalog declarative and the variants directionally predictable
(raise a contribution knob -> more contributions and more net cash flow; widen
the AVA -> smaller funded gap; etc.).

Metric flattening
-----------------
``run_deterministic_scenario`` returns one row per baseline metric; we flatten
to ``"det.<metric>.baseline|value|delta"``. ``compute_derived_metrics`` returns
one observation per ``(plan, period, derived_metric)`` group; we flatten to
``"derived.<plan>.<period>.<metric>"``. The whole run is one flat
``dict[str, float]``.
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]

# The deterministic-scenario metrics flattened per row (kit coverage space).
DET_METRICS = (
    "funded_ratio",
    "employer_contributions",
    "employee_contributions",
    "fee_rate",
    "net_return",
)
DET_FIELDS = ("baseline", "value", "delta")

# The derived metrics compute_derived_metrics can emit.
DERIVED_METRICS = (
    "funded_gap_usd",
    "unfunded_ratio",
    "net_cash_flow_usd",
    "contribution_to_benefit_ratio",
)


# ---------------------------------------------------------------------------
# Patch DSL
# ---------------------------------------------------------------------------


def _patch_facts(facts: dict[str, Any], kind: str, key: str, field: str, value: float) -> None:
    rows_field = "core_metric_rows" if kind == "core" else "cash_flow_rows"
    key_field = "metric_name" if kind == "core" else "cash_flow_id"
    for row in facts.get(rows_field, []):
        if row.get(key_field) == key:
            row[field] = float(value)


def apply_patch(base: dict[str, Any], patch: list[dict[str, Any]] | None) -> dict[str, Any]:
    """Return a deep copy of ``base`` with ``patch`` operations applied."""
    spec = copy.deepcopy(base)
    spec.setdefault("scenario", {})
    spec["scenario"].setdefault("macro_shocks", {})
    spec.setdefault("facts", {})
    for step in patch or []:
        op = step["op"]
        if op == "set_baseline":
            spec["baseline_metrics"][step["metric"]] = float(step["value"])
        elif op == "set_shock":
            spec["scenario"]["macro_shocks"][step["metric"]] = float(step["value"])
        elif op == "set_knob":
            spec["scenario"][step["knob"]] = float(step["value"])
        elif op == "set_fact":
            _patch_facts(
                spec["facts"], step["kind"], step["key"], step["field"], float(step["value"])
            )
        else:  # pragma: no cover - guards against catalog typos
            raise ValueError(f"unknown patch op: {op!r}")
    return spec


# ---------------------------------------------------------------------------
# Compute + flatten
# ---------------------------------------------------------------------------


def _run_deterministic(scenario_id: str, spec: dict[str, Any]) -> dict[str, float]:
    from pension_data.quant.scenarios import (
        ScenarioInput,
        ScenarioRunConfig,
        run_deterministic_scenario,
    )

    knobs = spec.get("scenario", {})
    scenario = ScenarioInput(
        name=str(knobs.get("name") or scenario_id),
        macro_shocks={str(k): float(v) for k, v in knobs.get("macro_shocks", {}).items()},
        contribution_delta=float(knobs.get("contribution_delta", 0.0)),
        fee_delta_bps=float(knobs.get("fee_delta_bps", 0.0)),
        return_override=(
            None if knobs.get("return_override") is None else float(knobs["return_override"])
        ),
    )
    result = run_deterministic_scenario(
        plan_id=str(spec.get("plan_id", "CA-PERS")),
        plan_period=str(spec.get("plan_period", "FY2024")),
        baseline_metrics={str(k): float(v) for k, v in spec["baseline_metrics"].items()},
        scenario=scenario,
        config=ScenarioRunConfig(module_version=str(spec.get("module_version", "v0.1.0"))),
        source_snapshot_id=str(spec.get("source_snapshot_id", "snapshot:baseline")),
    )
    flat: dict[str, float] = {}
    for row in result.rows:
        flat[f"det.{row.metric_name}.baseline"] = float(row.baseline_value)
        flat[f"det.{row.metric_name}.value"] = float(row.scenario_value)
        flat[f"det.{row.metric_name}.delta"] = float(row.delta_value)
    return flat


def _run_derived(spec: dict[str, Any]) -> dict[str, float]:
    from pension_data.quant.metric_engine import compute_derived_metrics

    facts = spec.get("facts", {})
    observations = compute_derived_metrics(
        core_metric_rows=facts.get("core_metric_rows", ()),
        cash_flow_rows=facts.get("cash_flow_rows", ()),
    )
    flat: dict[str, float] = {}
    for obs in observations:
        flat[f"derived.{obs.plan_id}.{obs.plan_period}.{obs.metric_name}"] = float(obs.value)
    return flat


def run_scenario(scenario: dict[str, Any], base: dict[str, Any]) -> dict[str, float]:
    """Apply a scenario's patch to the base fixture, compute both surfaces, flatten.

    Returns a flat ``dict`` combining the deterministic-scenario rows
    (``det.<metric>.<field>``) and the derived-metric observations
    (``derived.<plan>.<period>.<metric>``) -> float. Both surfaces are
    deterministic (no RNG, sorted output), so the flattened dict is stable.
    """
    spec = apply_patch(base, scenario.get("patch"))
    flat: dict[str, float] = {}
    flat.update(_run_deterministic(scenario["id"], spec))
    flat.update(_run_derived(spec))
    return flat


def metric_names() -> list[str]:
    names = [f"det.{m}.{f}" for m in DET_METRICS for f in DET_FIELDS]
    return names
