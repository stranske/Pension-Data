"""Pension-Data app behavior baseline kit.

Built on the shared ``baseline_kit`` package -- this directory contains only the
app-specific pieces (adapter, catalog, invariant bounds). The generic harness
(directional engine, invariant assertion, golden glue, coverage manifest) is
imported from ``baseline_kit``, the same core the TMP / PAEM / trip-planner /
Counter_Risk kits use.

Target surface: two deterministic compute functions in the quant layer (no DB,
no network, no LLM), so baselines here are stable:

  * ``pension_data.quant.scenarios.run_deterministic_scenario`` -- applies a
    scenario's knobs (macro shocks, contribution delta, fee delta, return
    override) to a baseline-metrics dict and reports baseline/scenario/delta per
    metric. This is the *directional* surface: a knob moves the metric it
    targets in a known direction.

  * ``pension_data.quant.metric_engine.compute_derived_metrics`` -- reduces
    staged plan facts (AAL/AVA + cash-flow rows) to derived economic metrics
    (funded gap, unfunded ratio, net cash flow, contribution-to-benefit ratio).
    This is the *invariant* surface: the derived values obey exact economic
    identities (funded_gap = aal - ava, etc.).

Both surfaces are flattened into one ``dict[str, float]`` per scenario (see
``adapter.run_scenario``) -- exactly what the kit's golden / directional /
coverage machinery consumes.
"""
