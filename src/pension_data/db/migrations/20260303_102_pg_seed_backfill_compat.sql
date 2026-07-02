-- PostgreSQL backward-compatible seed backfill for pre-bitemporal staging rows.
-- Issue: #128

UPDATE staging_core_metrics
SET
  plan_period = COALESCE(NULLIF(plan_period, ''), 'UNKNOWN'),
  benchmark_version = COALESCE(NULLIF(benchmark_version, ''), 'v1'),
  ingestion_date = COALESCE(ingestion_date, effective_date),
  valid_from = COALESCE(valid_from, effective_date),
  asserted_at = COALESCE(asserted_at, ingestion_date, effective_date),
  restated = superseded_at IS NOT NULL
WHERE
  plan_period IS NULL
  OR plan_period = ''
  OR benchmark_version IS NULL
  OR benchmark_version = ''
  OR ingestion_date IS NULL
  OR valid_from IS NULL
  OR asserted_at IS NULL
  OR (
    superseded_at IS NOT NULL
    AND restated IS DISTINCT FROM TRUE
  );

UPDATE staging_cash_flows
SET
  plan_period = COALESCE(NULLIF(plan_period, ''), 'UNKNOWN'),
  benchmark_version = COALESCE(NULLIF(benchmark_version, ''), 'v1'),
  ingestion_date = COALESCE(ingestion_date, effective_date)
WHERE
  plan_period IS NULL
  OR plan_period = ''
  OR benchmark_version IS NULL
  OR benchmark_version = ''
  OR ingestion_date IS NULL;
