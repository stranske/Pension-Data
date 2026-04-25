-- Extended staging tables for all domain models beyond core facts.
-- Issue: gap-remediation

-- ── Funded / Actuarial ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging_funded_actuarial (
  fact_id               TEXT PRIMARY KEY,
  plan_id               TEXT NOT NULL,
  plan_period           TEXT NOT NULL,
  metric_name           TEXT NOT NULL,
  as_reported_value     REAL,
  normalized_value      REAL,
  as_reported_unit      TEXT,
  normalized_unit       TEXT,
  effective_date        TEXT NOT NULL,
  ingestion_date        TEXT NOT NULL,
  source_document_id    TEXT NOT NULL,
  source_url            TEXT,
  extraction_method     TEXT NOT NULL,
  confidence            REAL NOT NULL,
  parser_version        TEXT NOT NULL,
  evidence_refs         TEXT
);

-- ── Investment Allocations ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging_allocations (
  allocation_id         TEXT PRIMARY KEY,
  plan_id               TEXT NOT NULL,
  plan_period           TEXT NOT NULL,
  category              TEXT NOT NULL,
  as_reported_percent   REAL,
  normalized_weight     REAL,
  as_reported_amount    REAL,
  normalized_amount_usd REAL,
  effective_date        TEXT NOT NULL,
  ingestion_date        TEXT NOT NULL,
  source_document_id    TEXT NOT NULL,
  evidence_refs         TEXT
);

-- ── Manager Fees ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging_manager_fees (
  fee_id                TEXT PRIMARY KEY,
  plan_id               TEXT NOT NULL,
  plan_period           TEXT NOT NULL,
  manager_name          TEXT NOT NULL,
  fee_type              TEXT NOT NULL,
  as_reported_rate_pct  REAL,
  normalized_rate       REAL,
  as_reported_amount    REAL,
  normalized_amount_usd REAL,
  completeness          TEXT NOT NULL,
  effective_date        TEXT NOT NULL,
  ingestion_date        TEXT NOT NULL,
  source_document_id    TEXT NOT NULL,
  evidence_refs         TEXT
);

-- ── Investment Positions ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging_positions (
  position_id           TEXT PRIMARY KEY,
  plan_id               TEXT NOT NULL,
  plan_period           TEXT NOT NULL,
  manager_name          TEXT NOT NULL,
  fund_name             TEXT,
  commitment            REAL,
  unfunded              REAL,
  market_value          REAL,
  completeness          TEXT NOT NULL,
  manager_canonical_id  TEXT,
  fund_canonical_id     TEXT,
  linkage_status        TEXT NOT NULL DEFAULT 'resolved',
  known_not_invested    INTEGER NOT NULL DEFAULT 0,
  confidence            REAL NOT NULL DEFAULT 1.0,
  evidence_refs         TEXT
);

-- ── Risk Exposures ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging_risk_exposures (
  exposure_id           TEXT PRIMARY KEY,
  plan_id               TEXT NOT NULL,
  plan_period           TEXT NOT NULL,
  disclosure_type       TEXT NOT NULL,
  metric_name           TEXT NOT NULL,
  observation_kind      TEXT NOT NULL,
  value_usd             REAL,
  value_ratio           REAL,
  as_reported_text      TEXT NOT NULL,
  confidence            REAL NOT NULL,
  evidence_refs         TEXT
);

-- ── Manager Lifecycle ───────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging_manager_lifecycle (
  event_id              TEXT PRIMARY KEY,
  plan_id               TEXT NOT NULL,
  plan_period           TEXT NOT NULL,
  manager_name          TEXT NOT NULL,
  fund_name             TEXT,
  event_type            TEXT NOT NULL,
  basis                 TEXT NOT NULL,
  confidence            REAL NOT NULL,
  evidence_refs         TEXT,
  manager_canonical_id  TEXT,
  fund_canonical_id     TEXT
);

-- ── Financial Flows ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging_financial_flows (
  flow_id                         TEXT PRIMARY KEY,
  plan_id                         TEXT NOT NULL,
  plan_period                     TEXT NOT NULL,
  effective_period                TEXT,
  reported_at                     TEXT,
  source_document_id              TEXT NOT NULL,
  beginning_aum_usd               REAL,
  ending_aum_usd                  REAL,
  employer_contributions_usd      REAL,
  employee_contributions_usd      REAL,
  benefit_payments_usd            REAL,
  refunds_usd                     REAL,
  net_external_cash_flow_usd      REAL,
  net_external_cash_flow_rate_pct REAL,
  consistency_gap_usd             REAL,
  disclosure_level                TEXT NOT NULL,
  evidence_refs                   TEXT
);

-- ── Consultant Attribution ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging_consultant_attribution (
  attribution_id          TEXT PRIMARY KEY,
  plan_id                 TEXT NOT NULL,
  plan_period             TEXT NOT NULL,
  consultant_name         TEXT NOT NULL,
  consultant_canonical_id TEXT,
  linkage_status          TEXT NOT NULL,
  recommendation_topic    TEXT,
  observed_outcome        TEXT,
  strength                TEXT NOT NULL,
  confidence              REAL NOT NULL,
  evidence_refs           TEXT
);

-- ── Registry ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS registry_systems (
  stable_id                   TEXT PRIMARY KEY,
  legal_name                  TEXT NOT NULL,
  short_name                  TEXT NOT NULL,
  system_type                 TEXT NOT NULL,
  jurisdiction                TEXT NOT NULL,
  jurisdiction_type           TEXT NOT NULL,
  identity_key                TEXT NOT NULL UNIQUE,
  in_state_employee_universe  INTEGER NOT NULL DEFAULT 0,
  in_sampled_50               INTEGER NOT NULL DEFAULT 0
);

-- ── Canonical Entities ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS canonical_entities (
  stable_id             TEXT PRIMARY KEY,
  entity_type           TEXT NOT NULL,
  display_name          TEXT NOT NULL,
  normalized_name       TEXT NOT NULL,
  normalized_key_fields TEXT,
  merged_into           TEXT,
  created_at            TEXT NOT NULL,
  updated_at            TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS entity_source_links (
  link_id               TEXT PRIMARY KEY,
  stable_entity_id      TEXT NOT NULL,
  source_record_id      TEXT NOT NULL,
  source_table          TEXT NOT NULL,
  evidence_refs         TEXT
);

-- ── Entity Lineage ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS entity_lineage_events (
  event_id              TEXT PRIMARY KEY,
  event_type            TEXT NOT NULL,
  source_entity_ids     TEXT,
  target_entity_ids     TEXT,
  occurred_at           TEXT NOT NULL,
  actor                 TEXT NOT NULL,
  rationale             TEXT NOT NULL
);

-- ── API Keys ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS api_keys (
  key_id                TEXT PRIMARY KEY,
  key_hash              TEXT NOT NULL,
  hash_scheme           TEXT NOT NULL DEFAULT 'sha256',
  scopes                TEXT NOT NULL,
  status                TEXT NOT NULL DEFAULT 'active',
  created_at            TEXT NOT NULL,
  label                 TEXT,
  revoked_at            TEXT,
  revoked_reason        TEXT,
  rotated_from          TEXT,
  rotated_to            TEXT
);

-- ── Raw Artifacts ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS raw_artifacts (
  artifact_id               TEXT PRIMARY KEY,
  plan_id                   TEXT NOT NULL,
  plan_period               TEXT NOT NULL,
  source_url                TEXT NOT NULL,
  fetched_at                TEXT NOT NULL,
  mime_type                 TEXT NOT NULL,
  byte_size                 INTEGER NOT NULL,
  checksum_sha256           TEXT NOT NULL,
  is_active                 INTEGER NOT NULL DEFAULT 1,
  supersedes_artifact_id    TEXT,
  superseded_by_artifact_id TEXT,
  first_seen_at             TEXT NOT NULL,
  last_seen_at              TEXT NOT NULL
);

-- ── Provenance ──────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS evidence_references (
  evidence_ref_id       TEXT PRIMARY KEY,
  report_id             TEXT NOT NULL,
  source_document_id    TEXT NOT NULL,
  raw_ref               TEXT NOT NULL,
  page_number           INTEGER,
  section_hint          TEXT,
  snippet_anchor        TEXT
);

CREATE TABLE IF NOT EXISTS metric_evidence_links (
  link_id               TEXT PRIMARY KEY,
  metric_row_id         TEXT NOT NULL,
  metric_family         TEXT NOT NULL,
  metric_name           TEXT NOT NULL,
  evidence_ref_id       TEXT NOT NULL
);

-- ── Review Queue (Extraction) ───────────────────────────────────────

CREATE TABLE IF NOT EXISTS extraction_review_queue (
  queue_id              TEXT PRIMARY KEY,
  row_id                TEXT NOT NULL,
  plan_id               TEXT NOT NULL,
  plan_period           TEXT NOT NULL,
  metric_name           TEXT NOT NULL,
  confidence            REAL NOT NULL,
  routing_outcome       TEXT NOT NULL,
  priority              TEXT NOT NULL,
  state                 TEXT NOT NULL DEFAULT 'new',
  created_at            TEXT NOT NULL,
  updated_at            TEXT NOT NULL,
  evidence_refs         TEXT
);

-- ── Review Queue (Entities) ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS entity_review_queue (
  queue_id              TEXT PRIMARY KEY,
  candidate_id          TEXT NOT NULL,
  source_name           TEXT NOT NULL,
  entity_type           TEXT NOT NULL,
  plan_id               TEXT NOT NULL,
  plan_period           TEXT NOT NULL,
  candidate_entity_ids  TEXT,
  provenance_refs       TEXT,
  confidence            REAL,
  state                 TEXT NOT NULL DEFAULT 'new',
  resolution_action     TEXT,
  resolved_entity_ids   TEXT,
  created_at            TEXT NOT NULL,
  updated_at            TEXT NOT NULL
);

-- ── Discovered Inventory ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS discovered_inventory (
  inventory_id                    TEXT PRIMARY KEY,
  plan_id                         TEXT NOT NULL,
  plan_year                       INTEGER,
  document_type                   TEXT NOT NULL,
  source_url                      TEXT NOT NULL,
  source_authority_tier           TEXT NOT NULL,
  manager_disclosure_available    INTEGER NOT NULL DEFAULT 0,
  consultant_disclosure_available INTEGER NOT NULL DEFAULT 0
);

-- ── Indexes ─────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_funded_actuarial_plan       ON staging_funded_actuarial(plan_id, plan_period);
CREATE INDEX IF NOT EXISTS idx_allocations_plan            ON staging_allocations(plan_id, plan_period);
CREATE INDEX IF NOT EXISTS idx_manager_fees_plan           ON staging_manager_fees(plan_id, plan_period);
CREATE INDEX IF NOT EXISTS idx_positions_plan              ON staging_positions(plan_id, plan_period);
CREATE INDEX IF NOT EXISTS idx_risk_exposures_plan         ON staging_risk_exposures(plan_id, plan_period);
CREATE INDEX IF NOT EXISTS idx_lifecycle_plan              ON staging_manager_lifecycle(plan_id, plan_period);
CREATE INDEX IF NOT EXISTS idx_financial_flows_plan        ON staging_financial_flows(plan_id, plan_period);
CREATE INDEX IF NOT EXISTS idx_consultant_attribution_plan ON staging_consultant_attribution(plan_id, plan_period);
CREATE INDEX IF NOT EXISTS idx_core_metrics_plan           ON staging_core_metrics(plan_id, plan_period);
CREATE INDEX IF NOT EXISTS idx_core_metrics_temporal       ON staging_core_metrics(effective_date, ingestion_date);
CREATE INDEX IF NOT EXISTS idx_cash_flows_plan             ON staging_cash_flows(plan_id, plan_period);
CREATE INDEX IF NOT EXISTS idx_relationships_plan          ON staging_manager_fund_vehicle_relationships(plan_id, plan_period);
CREATE INDEX IF NOT EXISTS idx_consultant_engagements_plan ON staging_consultant_engagements(plan_id, plan_period);
CREATE INDEX IF NOT EXISTS idx_entities_type               ON canonical_entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_entity_links_entity         ON entity_source_links(stable_entity_id);
CREATE INDEX IF NOT EXISTS idx_evidence_refs_report        ON evidence_references(report_id);
CREATE INDEX IF NOT EXISTS idx_metric_evidence_metric      ON metric_evidence_links(metric_row_id);
CREATE INDEX IF NOT EXISTS idx_extraction_review_state     ON extraction_review_queue(state);
CREATE INDEX IF NOT EXISTS idx_entity_review_state         ON entity_review_queue(state);
CREATE INDEX IF NOT EXISTS idx_artifacts_plan              ON raw_artifacts(plan_id, plan_period);
CREATE INDEX IF NOT EXISTS idx_inventory_plan              ON discovered_inventory(plan_id);
