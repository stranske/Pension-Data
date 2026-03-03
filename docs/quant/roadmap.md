# Quantitative Analysis Roadmap

Parent issue: #120

## Objective

Sequence quantitative delivery so each module is directly grounded in extracted Pension-Data facts and can be consumed by web and desktop UX surfaces without contract drift.

## Delivery Sequence

1. `#121` Metric Engine and Derived Analytics Catalog
   - Inputs: `staging_core_metrics`, `staging_cash_flows`, entity linkage outputs.
   - Outputs: canonical derived metrics with formula metadata, units, and provenance refs.
   - Exit criteria: deterministic derived metrics and regression coverage for formula correctness.
2. `#122` Scenario / Stress / Simulation Layer
   - Inputs: metric-engine outputs from `#121`.
   - Outputs: baseline-vs-scenario comparisons plus optional seeded simulation envelopes.
   - Exit criteria: reproducible scenario runs with config hash + seed + version capture.
3. `#123` Attribution / Optimization / Experiment Tracking
   - Inputs: metric and scenario outputs from `#121/#122`.
   - Outputs: attribution decomposition, constrained optimization experiments, auditable run comparisons.
   - Exit criteria: experiment registry records full input/output/reproducibility metadata.

## Alignment to Available Facts and Provenance

- Quant modules must consume persisted facts only (no private side channels).
- Every series/table output must carry provenance references back to source artifacts where possible.
- Low-confidence upstream facts must remain visible through confidence-aware aggregation rules.

## Shared Contracts

- Canonical quant payload contracts live in `src/pension_data/quant/contracts.py`.
- UX-facing documentation for payload shape and reproducibility requirements lives in:
  - `docs/contracts/quant-data-model-contract.md`

## Reproducibility Guardrails

- Every quant result must include:
  - `run_id`
  - `config_hash`
  - `code_version`
  - `input_snapshot_id`
  - `generated_at`
  - `source_artifact_ids`
- Seed is required for stochastic/simulation mode and optional for deterministic-only runs.
