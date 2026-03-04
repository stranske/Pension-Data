# Quant Data Model Contract

Defines the shared quantitative output contract for browser UI, desktop app, and LangChain explanation layers.

## Source of Truth

- Python contract definitions: `src/pension_data/quant/contracts.py`
- Contract tests: `tests/quant/test_contracts.py`

## Contract Entities

1. `ReproducibilityEnvelope`
   - Required for every quant scenario/experiment output.
   - Required fields:
     - `run_id`
     - `config_hash`
     - `code_version`
     - `input_snapshot_id`
     - `generated_at`
     - `source_artifact_ids` (non-empty)
   - Conditional field:
     - `seed` required for stochastic/simulation runs.
2. `QuantDataPoint`
   - `x_label`, `y_value`, `y_unit`, `confidence`, `provenance_refs`
3. `QuantSeriesContract`
   - `series_id`, `metric_name`, `label`, `chart_kind`, `points`
4. `QuantScenarioContract`
   - `scenario_id`, `scenario_label`, `module`, `baseline_scenario_id`, `reproducibility`, `series`, `warnings`
5. `QuantWorkspaceContract`
   - `plan_id`, `plan_period`, `as_of_date`, `module`, `scenarios`

## Module Values

`module` values are constrained to:

- `metric_engine`
- `scenario_analysis`
- `attribution_optimization`

## UX Interoperability Rules

- Web and desktop clients should render `QuantSeriesContract` by `chart_kind` without schema translation.
- `provenance_refs` should be displayed as citations/tooltips where available.
- Warnings should be surfaced as non-blocking banners with drill-through detail.

## Reproducibility Validation

Use `missing_reproducibility_fields(...)` before publishing quant results:

- `requires_seed=False` for deterministic-only modules (e.g., metric engine).
- `requires_seed=True` for simulation-enabled workflows.
