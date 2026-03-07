# Quant Metric Catalog

Issue: #121

## Purpose

Define derived metrics computed from Pension-Data core facts and cash-flow rows with explicit formula metadata, units, and data requirements.

## Catalog

| Metric | Unit | Formula | Required Inputs |
|---|---|---|---|
| `funded_gap_usd` | `usd` | `aal_usd - ava_usd` | `aal_usd`, `ava_usd` |
| `unfunded_ratio` | `ratio` | `(aal_usd - ava_usd) / aal_usd` | `aal_usd`, `ava_usd` |
| `net_cash_flow_usd` | `usd` | `employer_contributions_normalized + employee_contributions_normalized + benefit_payments_normalized + refunds_normalized` | `employer_contributions_normalized`, `employee_contributions_normalized`, `benefit_payments_normalized`, `refunds_normalized` |
| `contribution_to_benefit_ratio` | `ratio` | `(employer_contributions_normalized + employee_contributions_normalized) / ABS(benefit_payments_normalized)` | `employer_contributions_normalized`, `employee_contributions_normalized`, `benefit_payments_normalized` |

## Provenance and Lineage

- Every derived metric observation carries:
  - source identifiers (`source_fact_ids`; cash-flow derived metrics include the cash-flow row id there)
  - provenance references (`provenance_refs`)
  - explicit lineage formula (`lineage_formula`)
  - bounded confidence in `[0, 1]` when confidence input is available
- Confidence-aware weighting is used for aggregate rollups:
  - if `confidence` exists, use it as weight
  - otherwise default weight is `1.0`

## UI/Workspace Contract

- `build_metric_series_contracts(...)` converts derived observations into deterministic
  `QuantSeriesContract` payloads for chart/workspace rendering.

## Implementation

- Engine module: `src/pension_data/quant/metric_engine.py`
- Regression tests: `tests/quant/test_metric_engine.py`
