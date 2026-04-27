-- PostgreSQL core fact staging schema with bitemporal + dual-reporting columns.
-- Issue: #128

CREATE TABLE IF NOT EXISTS staging_core_metrics (
  fact_id TEXT PRIMARY KEY,
  plan_id TEXT NOT NULL,
  plan_period TEXT NOT NULL,
  metric_family TEXT NOT NULL,
  metric_name TEXT NOT NULL,
  as_reported_value DOUBLE PRECISION,
  normalized_value DOUBLE PRECISION,
  as_reported_unit TEXT,
  normalized_unit TEXT,
  manager_name TEXT,
  fund_name TEXT,
  vehicle_name TEXT,
  relationship_completeness TEXT,
  confidence DOUBLE PRECISION,
  evidence_refs JSONB,
  effective_date TIMESTAMPTZ NOT NULL,
  ingestion_date TIMESTAMPTZ NOT NULL,
  benchmark_version TEXT NOT NULL,
  source_document_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS staging_cash_flows (
  cash_flow_id TEXT PRIMARY KEY,
  plan_id TEXT NOT NULL,
  plan_period TEXT NOT NULL,
  beginning_aum_as_reported DOUBLE PRECISION,
  beginning_aum_normalized DOUBLE PRECISION,
  beginning_aum_as_reported_unit TEXT,
  beginning_aum_normalized_unit TEXT,
  ending_aum_as_reported DOUBLE PRECISION,
  ending_aum_normalized DOUBLE PRECISION,
  ending_aum_as_reported_unit TEXT,
  ending_aum_normalized_unit TEXT,
  employer_contributions_as_reported DOUBLE PRECISION,
  employer_contributions_normalized DOUBLE PRECISION,
  employer_contributions_as_reported_unit TEXT,
  employer_contributions_normalized_unit TEXT,
  employee_contributions_as_reported DOUBLE PRECISION,
  employee_contributions_normalized DOUBLE PRECISION,
  employee_contributions_as_reported_unit TEXT,
  employee_contributions_normalized_unit TEXT,
  benefit_payments_as_reported DOUBLE PRECISION,
  benefit_payments_normalized DOUBLE PRECISION,
  benefit_payments_as_reported_unit TEXT,
  benefit_payments_normalized_unit TEXT,
  refunds_as_reported DOUBLE PRECISION,
  refunds_normalized DOUBLE PRECISION,
  refunds_as_reported_unit TEXT,
  refunds_normalized_unit TEXT,
  confidence DOUBLE PRECISION,
  evidence_refs JSONB,
  effective_date TIMESTAMPTZ NOT NULL,
  ingestion_date TIMESTAMPTZ NOT NULL,
  benchmark_version TEXT NOT NULL,
  source_document_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS staging_manager_fund_vehicle_relationships (
  relationship_id TEXT PRIMARY KEY,
  plan_id TEXT NOT NULL,
  plan_period TEXT NOT NULL,
  manager_name TEXT NOT NULL,
  fund_name TEXT,
  vehicle_name TEXT,
  relationship_completeness TEXT NOT NULL,
  known_not_invested BOOLEAN NOT NULL DEFAULT FALSE,
  effective_date TIMESTAMPTZ NOT NULL,
  ingestion_date TIMESTAMPTZ NOT NULL,
  benchmark_version TEXT NOT NULL,
  source_document_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS staging_consultant_engagements (
  engagement_id TEXT PRIMARY KEY,
  plan_id TEXT NOT NULL,
  plan_period TEXT NOT NULL,
  consultant_name TEXT NOT NULL,
  role_description TEXT NOT NULL,
  recommendation_topic TEXT,
  recommendation_text TEXT,
  attribution_outcome TEXT,
  relationship_completeness TEXT NOT NULL,
  effective_date TIMESTAMPTZ NOT NULL,
  ingestion_date TIMESTAMPTZ NOT NULL,
  benchmark_version TEXT NOT NULL,
  source_document_id TEXT NOT NULL
);

CREATE OR REPLACE VIEW curated_metric_facts AS
SELECT
  plan_id,
  plan_period,
  metric_family,
  metric_name,
  normalized_value,
  normalized_unit,
  manager_name,
  fund_name,
  vehicle_name,
  effective_date,
  ingestion_date,
  benchmark_version,
  source_document_id
FROM staging_core_metrics
WHERE normalized_value IS NOT NULL
  AND normalized_unit IS NOT NULL
  AND BTRIM(normalized_unit) <> '';
