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
- Static review findings should be published first to
  `docs/data/reviewable-findings/extraction-quality-dashboard.json` using
  `docs/data/reviewable-findings/findings.schema.json` so UI and LangChain consumers share one
  artifact path before broader quant workspace generation.

## Reproducibility Validation

Use `missing_reproducibility_fields(...)` before publishing quant results:

- `requires_seed=False` for deterministic-only modules (e.g., metric engine).
- `requires_seed=True` for simulation-enabled workflows.

## Completed-pilot metric producer

Run locally from the repository (the default output is relative to the working directory):

```sh
python scripts/web/build_quant_workspace.py --pilot-run-dir /absolute/path/to/completed/pilot-run
```

This publishes `outputs/web/quant-workspace.generated.json`, or the explicit `--out` path.
It produces the `metric_engine` contract only. The existing workspace bundle, reviewable
findings schema, browser UI and desktop consumers do not yet consume or render this artifact.
No scenario execution, chart rendering, database/service, LLM explanation or UI readiness
is implied.

Eligibility is deliberately narrow: a `ledger_status=success` manifest, one matching
plan/period/source document, and one current, unsuperseded `aal_usd` and `ava_usd` fact each.
Required facts must have persisted fact IDs, finite normalized USD amounts, page evidence,
positive AAL, nonnegative AVA, and valid confidence or an explicit null. Conflicting or
duplicate required facts are rejected rather than selected by row order. Artifact paths
must resolve inside that pilot run; absolute, run-relative and legacy cwd-rooted pilot
paths are supported. Missing, malformed or escaped declared artifacts fail closed.

The optional `financial_flow_rows_json` artifact is read when declared. Its normalized
`employer_contributions_usd`, `employee_contributions_usd`, signed `benefit_payments_usd`
and signed `refunds_usd` map to the metric engine's corresponding `*_normalized` inputs.
There may be at most one flow row for the selected plan/period. Missing optional amounts
remain absent; omitted cash-flow metrics have explicit warnings, never fabricated zeros.
An undeclared or empty cash-flow artifact still permits the required funded metrics.

The top-level contract carries declared lineage extensions:

- `input_artifacts` maps manifest keys to the actual captured relative path, byte SHA-256
  and content-addressed artifact ID. Reproducibility `source_artifact_ids` refer to these
  artifact IDs, including the captured manifest, rather than to metric fact IDs.
- `fact_lineage` maps persisted core fact IDs to their artifact/row offset, source document,
  confidence and page evidence. Pilot financial-flow rows have no persisted fact ID;
  their explicitly synthetic identity is the exact artifact digest plus row offset.
- `metric_lineage` retains the real engine observations, formulas and `source_fact_ids`.
- `producer_config` records the eligibility/catalog configuration actually hashed for
  `config_hash`. `input_snapshot_id` hashes the captured artifact descriptors, and
  `code_version` hashes the producer, metric-engine and contract source bytes.

Every scenario passes `missing_reproducibility_fields(requires_seed=False)`. The full
payload is validated before an atomic output replacement; invalid inputs, missing
reproducibility or nonfinite/empty metrics preserve the previous output. Publication
inside the pilot input directory is refused.

Validate with `python -m pytest tests/quant/test_contracts.py
tests/quant/test_metric_engine.py tests/web/test_workspace_bundle_quant_contract.py
tests/web/test_build_workspace_bundle.py tests/export/test_workspace_bundle.py -q`.
The producer integration tests build completed artifacts through `run_one_pdf_pilot`,
assert numeric input sensitivity and provenance, and run the real CLI with missing
reproducibility fields to prove nonzero exit and byte-preservation of the prior artifact.
