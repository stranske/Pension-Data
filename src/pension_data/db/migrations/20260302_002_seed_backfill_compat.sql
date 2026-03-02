-- Backward-compatible seed backfill for pre-bitemporal staging rows.
-- Issue: #20

UPDATE staging_core_metrics
SET
  plan_period = COALESCE(NULLIF(plan_period, ''), 'UNKNOWN'),
  benchmark_version = COALESCE(NULLIF(benchmark_version, ''), 'v1'),
  ingestion_date = COALESCE(NULLIF(ingestion_date, ''), effective_date)
WHERE
  plan_period = ''
  OR benchmark_version = ''
  OR ingestion_date = '';

UPDATE staging_cash_flows
SET
  plan_period = COALESCE(NULLIF(plan_period, ''), 'UNKNOWN'),
  benchmark_version = COALESCE(NULLIF(benchmark_version, ''), 'v1'),
  ingestion_date = COALESCE(NULLIF(ingestion_date, ''), effective_date)
WHERE
  plan_period = ''
  OR benchmark_version = ''
  OR ingestion_date = '';
