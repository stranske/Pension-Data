-- Core fact staging schema with bitemporal + dual-reporting columns.
-- Issue: #20

CREATE TABLE IF NOT EXISTS staging_core_metrics (
  fact_id TEXT PRIMARY KEY,
  plan_id TEXT NOT NULL,
  plan_period TEXT NOT NULL,
  metric_family TEXT NOT NULL,
  metric_name TEXT NOT NULL,
  as_reported_value REAL,
  normalized_value REAL,
  as_reported_unit TEXT,
  normalized_unit TEXT,
  manager_name TEXT,
  fund_name TEXT,
  vehicle_name TEXT,
  relationship_completeness TEXT,
  confidence REAL,
  evidence_refs TEXT,
  effective_date TEXT NOT NULL,
  ingestion_date TEXT NOT NULL,
  benchmark_version TEXT NOT NULL,
  source_document_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS staging_cash_flows (
  cash_flow_id TEXT PRIMARY KEY,
  plan_id TEXT NOT NULL,
  plan_period TEXT NOT NULL,
  beginning_aum_as_reported REAL,
  beginning_aum_normalized REAL,
  beginning_aum_as_reported_unit TEXT,
  beginning_aum_normalized_unit TEXT,
  ending_aum_as_reported REAL,
  ending_aum_normalized REAL,
  ending_aum_as_reported_unit TEXT,
  ending_aum_normalized_unit TEXT,
  employer_contributions_as_reported REAL,
  employer_contributions_normalized REAL,
  employer_contributions_as_reported_unit TEXT,
  employer_contributions_normalized_unit TEXT,
  employee_contributions_as_reported REAL,
  employee_contributions_normalized REAL,
  employee_contributions_as_reported_unit TEXT,
  employee_contributions_normalized_unit TEXT,
  benefit_payments_as_reported REAL,
  benefit_payments_normalized REAL,
  benefit_payments_as_reported_unit TEXT,
  benefit_payments_normalized_unit TEXT,
  refunds_as_reported REAL,
  refunds_normalized REAL,
  refunds_as_reported_unit TEXT,
  refunds_normalized_unit TEXT,
  confidence REAL,
  evidence_refs TEXT,
  effective_date TEXT NOT NULL,
  ingestion_date TEXT NOT NULL,
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
  known_not_invested INTEGER NOT NULL DEFAULT 0,
  effective_date TEXT NOT NULL,
  ingestion_date TEXT NOT NULL,
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
  effective_date TEXT NOT NULL,
  ingestion_date TEXT NOT NULL,
  benchmark_version TEXT NOT NULL,
  source_document_id TEXT NOT NULL
);

CREATE VIEW IF NOT EXISTS curated_metric_facts AS
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
  AND TRIM(normalized_unit) <> '';

CREATE VIEW IF NOT EXISTS curated_cash_flow_facts AS
SELECT
  plan_id,
  plan_period,
  beginning_aum_normalized,
  ending_aum_normalized,
  employer_contributions_normalized,
  employee_contributions_normalized,
  benefit_payments_normalized,
  refunds_normalized,
  effective_date,
  ingestion_date,
  benchmark_version,
  source_document_id
FROM staging_cash_flows
WHERE beginning_aum_normalized IS NOT NULL
  AND ending_aum_normalized IS NOT NULL
  AND employer_contributions_normalized IS NOT NULL
  AND employee_contributions_normalized IS NOT NULL
  AND benefit_payments_normalized IS NOT NULL
  AND refunds_normalized IS NOT NULL
  AND beginning_aum_normalized_unit IS NOT NULL
  AND ending_aum_normalized_unit IS NOT NULL
  AND employer_contributions_normalized_unit IS NOT NULL
  AND employee_contributions_normalized_unit IS NOT NULL
  AND benefit_payments_normalized_unit IS NOT NULL
  AND refunds_normalized_unit IS NOT NULL
  AND TRIM(beginning_aum_normalized_unit) <> ''
  AND TRIM(ending_aum_normalized_unit) <> ''
  AND TRIM(employer_contributions_normalized_unit) <> ''
  AND TRIM(employee_contributions_normalized_unit) <> ''
  AND TRIM(benefit_payments_normalized_unit) <> ''
  AND TRIM(refunds_normalized_unit) <> '';
