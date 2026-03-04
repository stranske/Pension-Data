# Attribution and Optimization Contract

Issue: #123

## Attribution

`compute_attribution(...)` emits:

- `AttributionRow.bucket`
- `AttributionRow.weight`
- `AttributionRow.return_rate`
- `AttributionRow.contribution`

Portfolio total return is the sum of row contributions.

`reconcile_attribution(...)` adds tolerance-based reconciliation against a source
aggregate with:

- `source_aggregate`
- `computed_total`
- `delta`
- `tolerance`
- `within_tolerance`

## Optimization Sandbox

`optimize_allocation(...)` supports:

- objective: maximize `expected_return - lambda * penalty`
- constraints:
  - per-bucket min/max weights
  - target total weight
  - configurable precision step
- sensitivity control via `sensitivity_lambda`

Output diagnostics:

- `objective_value`
- `expected_return`
- `penalty`
- `weights`
- `diagnostics.target_total_weight`
- `diagnostics.realized_total_weight`
- `diagnostics.total_weight_delta`
- `diagnostics.violated_bounds`
- `diagnostics.within_tolerance`

## Experiment Registry

`QuantExperimentRegistry` captures auditable records with:

- `experiment_id`
- `module`
- `seed`
- `input_hash`
- `output_hash`
- `artifact_links`
- optional `objective_value` and `total_return`

Comparison output:

- `objective_delta`
- `total_return_delta`

Export support:

- `QuantExperimentRegistry.export_json()`
- `QuantExperimentRegistry.export_json_file(path)`

Runner support:

- `QuantExperimentRunner.run_optimization_experiment(...)`
- `QuantExperimentRunner.run_attribution_experiment(...)`

Implementation:

- `src/pension_data/quant/attribution_optimization.py`
- `tests/quant/test_attribution_optimization.py`
