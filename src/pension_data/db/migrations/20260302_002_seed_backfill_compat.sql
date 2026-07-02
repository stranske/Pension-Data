-- Backward-compatible seed backfill for pre-bitemporal staging rows.
-- Issue: #20

UPDATE staging_core_metrics
SET
  plan_period = COALESCE(NULLIF(plan_period, ''), 'UNKNOWN'),
  benchmark_version = COALESCE(NULLIF(benchmark_version, ''), 'v1'),
  ingestion_date = COALESCE(NULLIF(ingestion_date, ''), effective_date),
  valid_from = COALESCE(NULLIF(valid_from, ''), effective_date),
  asserted_at = COALESCE(NULLIF(asserted_at, ''), ingestion_date, effective_date),
  restated = CASE WHEN superseded_at IS NULL OR TRIM(superseded_at) = '' THEN 0 ELSE 1 END
WHERE
  plan_period IS NULL
  OR plan_period = ''
  OR benchmark_version IS NULL
  OR benchmark_version = ''
  OR ingestion_date IS NULL
  OR ingestion_date = ''
  OR valid_from IS NULL
  OR valid_from = ''
  OR asserted_at IS NULL
  OR asserted_at = '';

UPDATE staging_cash_flows
SET
  plan_period = COALESCE(NULLIF(plan_period, ''), 'UNKNOWN'),
  benchmark_version = COALESCE(NULLIF(benchmark_version, ''), 'v1'),
  ingestion_date = COALESCE(NULLIF(ingestion_date, ''), effective_date)
WHERE
  plan_period IS NULL
  OR plan_period = ''
  OR benchmark_version IS NULL
  OR benchmark_version = ''
  OR ingestion_date IS NULL
  OR ingestion_date = '';
