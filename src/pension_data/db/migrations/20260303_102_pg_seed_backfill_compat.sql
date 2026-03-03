-- PostgreSQL backward-compatible seed backfill for pre-bitemporal staging rows.
-- Issue: #128

UPDATE staging_core_metrics
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
