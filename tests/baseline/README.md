# Pension-Data app behavior baseline kit

Scenario-driven wiring / sensibility / regression tests built on the shared
**`baseline_kit`** package. Only the app-specific pieces live here.

> Distinct from `tests/golden/` (the one-PDF-pilot / NL-SQL reference runs).
> This kit lives entirely under `tests/baseline/` and exercises the quant layer.

## Requires

`baseline_kit` (the shared core) must be importable. It lives in
`stranske/Workflows` under `packages/app-baseline-kit`:

```bash
pip install "app-baseline-kit @ git+https://github.com/stranske/Workflows.git#subdirectory=packages/app-baseline-kit"
```

It is declared in this repo's `pyproject.toml` `[project.optional-dependencies]
dev`, so `pip install -e ".[dev]"` pulls it (plus `pytest-regressions`, whose
`num_regression` fixture needs `numpy` + `pandas`).

## Target surfaces

Two **deterministic compute** functions in the quant layer (no DB / network / LLM):

- `pension_data.quant.scenarios.run_deterministic_scenario` — applies scenario
  knobs (macro shocks, contribution delta, fee delta bps, return override) to a
  baseline-metrics dict and reports `baseline` / `value` / `delta` per metric.
  This is the **directional** surface.
- `pension_data.quant.metric_engine.compute_derived_metrics` — reduces staged
  plan facts (AAL/AVA + cash-flow rows) to derived economic metrics
  (funded gap, unfunded ratio, net cash flow, contribution-to-benefit ratio).
  This is the **invariant** surface (exact economic identities).

## Layout

```
adapter.py                # base fixture + patch -> flat metrics dict (the only app glue)
catalog.yaml              # base fixture + scenario patches + directional checks
invariants.py             # economic identities/bounds -> baseline_kit.InvariantResult
test_golden.py            # golden master of each scenario's flattened metrics
test_directional.py       # metamorphic checks (contribute more -> contributions up; widen AVA -> gap down...)
test_invariants.py        # invariants on base + every scenario
test_coverage_manifest.py # metric-key coverage -> docs/reports/baseline-coverage.md
```

## Scenario model

A *scenario* is the base fixture (`catalog.yaml` `base`) with an optional
ordered `patch` applied. The patch DSL (`adapter.apply_patch`) supports
`set_baseline`, `set_shock`, `set_knob`, `set_fact` — enough to make each
variant directionally predictable (raise the contribution knob → more
contributions; widen AVA → smaller funded gap).

## Running

```bash
pytest tests/baseline/                                   # full suite
pytest tests/baseline/test_golden.py --force-regen       # re-bless after an intended change
BASELINE_REFRESH_REPORT=1 pytest tests/baseline/test_coverage_manifest.py  # refresh report
```

## Invariants enforced

Grounded in the formulas in `scenarios.py` and `metric_engine.py`:

Deterministic-scenario contract:
- `delta == value - baseline` for every metric
- reported `baseline` equals the supplied baseline level
- a metric no knob/shock targets is unchanged (`delta == 0`)
- `fee_rate.value == fee_rate.baseline + fee_delta_bps / 10000`
- `net_return.value == return_override` when an override is set

Derived-metric contract:
- `funded_gap_usd == aal_usd - ava_usd`
- `unfunded_ratio == funded_gap_usd / aal_usd`; `<= 1` when `ava >= 0`, `aal > 0`
- `net_cash_flow_usd == employer + employee + benefit + refunds`
- `contribution_to_benefit_ratio == (employer + employee) / abs(benefit)`; `>= 0`
  for non-negative contributions
