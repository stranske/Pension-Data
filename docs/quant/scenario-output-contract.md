# Scenario Output Contract

Issue: #122

## Scenario Input Schema

`ScenarioInput` fields:

- `name`
- `macro_shocks` (metric -> additive float shock)
- `contribution_delta`
- `fee_delta_bps`
- `return_override`

## Execution Modes

- `deterministic`: direct baseline-vs-scenario comparison
- `simulation`: seeded Monte Carlo around deterministic scenario values

## Result Contract

`ScenarioResult` includes:

- `mode`
- `scenario_name`
- `plan_id`
- `plan_period`
- `rows` (`ScenarioResultRow`)
- `reproducibility` (`ReproducibilityMetadata`)

Each `ScenarioResultRow` includes:

- `metric_name`
- `baseline_value`
- `scenario_value`
- `delta_value`

## Reproducibility Requirements

Every output must include:

- `run_id`
- `config_hash`
- `module_version`
- `source_snapshot_id`
- `random_seed` (required for simulation mode)

Implementation:

- `src/pension_data/quant/scenarios.py`
- `tests/quant/test_scenarios.py`
