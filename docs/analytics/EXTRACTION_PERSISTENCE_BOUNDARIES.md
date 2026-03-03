# Extraction Persistence Boundaries

Issue: #88

This document defines what "persisted" means for funded/actuarial and investment extraction flows introduced in Issues #21 and #22.

## Ownership

- Module owner: `src/pension_data/extract/persistence.py`
- Upstream extractors:
  - `src/pension_data/extract/actuarial/metrics.py`
  - `src/pension_data/extract/investment/allocation_fees.py`
  - `src/pension_data/extract/investment/manager_positions.py`
- Write-path responsibility:
  - Convert extractor dataclass outputs to deterministic staging/output artifact rows.
  - Preserve bitemporal fields and provenance metadata through the write step.
  - Persist extraction warnings to an explicit artifact path.

## Persistence Contract

The write path defines required columns for these artifacts:

- `staging_core_metrics`
  - Includes dual as-reported/normalized values, metric identity, relationship completeness, confidence, evidence refs, effective/ingestion dates, benchmark version, and source document provenance.
- `staging_manager_fund_vehicle_relationships`
  - Includes manager/fund relationship completeness, known-not-invested flag, evidence refs, effective/ingestion dates, benchmark version, and source provenance.
- `extraction_warnings`
  - Includes warning domain/code/severity, plan-period scope, optional manager/fund/metric references, message text, evidence refs, and available temporal/provenance fields.

## Bitemporal And Provenance Guarantees

- Bitemporal fields:
  - `effective_date`
  - `ingestion_date`
  - `benchmark_version`
- Provenance fields:
  - `source_document_id`
  - `source_url`
  - `evidence_refs`
  - parser/extraction method metadata where available (`parser_version`, `extraction_method`)

These fields are carried from extractor output into persisted rows without lossy transformation.

## Manager Holdings And Non-Disclosure Handling

- Disclosed manager holdings from `build_manager_fund_positions` persist into the same `staging_core_metrics` artifact path as `holding` metric rows (`commitment`, `unfunded`, `market_value`).
- Every manager-position row (including non-disclosure rows) persists into `staging_manager_fund_vehicle_relationships`.
- Explicit non-disclosure rows use sentinel manager identifiers when source rows do not include a manager name, so non-disclosure states are represented rather than dropped.

## Artifact Write Path

`write_extraction_persistence_artifacts(...)` writes deterministic JSON artifacts to:

- `extraction_persistence/persistence_contract.json`
- `extraction_persistence/staging_core_metrics.json`
- `extraction_persistence/staging_manager_fund_vehicle_relationships.json`
- `extraction_persistence/extraction_warnings.json`
