-- Extended staging tables for all domain models beyond core facts (PostgreSQL).
-- Issue: gap-remediation

-- ── Funded / Actuarial ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging_funded_actuarial (
  fact_id               TEXT PRIMARY KEY,
  plan_id               TEXT NOT NULL,
  plan_period           TEXT NOT NULL,
  metric_name           TEXT NOT NULL,
  as_reported_value     DOUBLE PRECISION,
  normalized_value      DOUBLE PRECISION,
  as_reported_unit      TEXT,
  normalized_unit       TEXT,
  effective_date        TIMESTAMPTZ NOT NULL,
  ingestion_date        TIMESTAMPTZ NOT NULL,
  source_document_id    TEXT NOT NULL,
  source_url            TEXT,
  extraction_method     TEXT NOT NULL,
  confidence            DOUBLE PRECISION NOT NULL,
  parser_version        TEXT NOT NULL,
  evidence_refs         JSONB
);

-- ── Investment Allocations ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging_allocations (
  allocation_id         TEXT PRIMARY KEY,
  plan_id               TEXT NOT NULL,
  plan_period           TEXT NOT NULL,
  category              TEXT NOT NULL,
  as_reported_percent   DOUBLE PRECISION,
  normalized_weight     DOUBLE PRECISION,
  as_reported_amount    DOUBLE PRECISION,
  normalized_amount_usd DOUBLE PRECISION,
  effective_date        TIMESTAMPTZ NOT NULL,
  ingestion_date        TIMESTAMPTZ NOT NULL,
  source_document_id    TEXT NOT NULL,
  evidence_refs         JSONB
);

-- ── Manager Fees ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging_manager_fees (
  fee_id                TEXT PRIMARY KEY,
  plan_id               TEXT NOT NULL,
  plan_period           TEXT NOT NULL,
  manager_name          TEXT NOT NULL,
  fee_type              TEXT NOT NULL,
  as_reported_rate_pct  DOUBLE PRECISION,
  normalized_rate       DOUBLE PRECISION,
  as_reported_amount    DOUBLE PRECISION,
  normalized_amount_usd DOUBLE PRECISION,
  completeness          TEXT NOT NULL,
  effective_date        TIMESTAMPTZ NOT NULL,
  ingestion_date        TIMESTAMPTZ NOT NULL,
  source_document_id    TEXT NOT NULL,
  evidence_refs         JSONB
);

-- ── Investment Positions ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging_positions (
  position_id           TEXT PRIMARY KEY,
  plan_id               TEXT NOT NULL,
  plan_period           TEXT NOT NULL,
  manager_name          TEXT NOT NULL,
  fund_name             TEXT,
  commitment            DOUBLE PRECISION,
  unfunded              DOUBLE PRECISION,
  market_value          DOUBLE PRECISION,
  completeness          TEXT NOT NULL,
  manager_canonical_id  TEXT,
  fund_canonical_id     TEXT,
  linkage_status        TEXT NOT NULL DEFAULT 'resolved',
  known_not_invested    BOOLEAN NOT NULL DEFAULT FALSE,
  confidence            DOUBLE PRECISION NOT NULL DEFAULT 1.0,
  evidence_refs         JSONB
);

-- ── Risk Exposures ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging_risk_exposures (
  exposure_id           TEXT PRIMARY KEY,
  plan_id               TEXT NOT NULL,
  plan_period           TEXT NOT NULL,
  disclosure_type       TEXT NOT NULL,
  metric_name           TEXT NOT NULL,
  observation_kind      TEXT NOT NULL,
  value_usd             DOUBLE PRECISION,
  value_ratio           DOUBLE PRECISION,
  as_reported_text      TEXT NOT NULL,
  confidence            DOUBLE PRECISION NOT NULL,
  evidence_refs         JSONB
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
  confidence            DOUBLE PRECISION NOT NULL,
  evidence_refs         JSONB,
  manager_canonical_id  TEXT,
  fund_canonical_id     TEXT
);

-- ── Financial Flows ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS staging_financial_flows (
  flow_id                         TEXT PRIMARY KEY,
  plan_id                         TEXT NOT NULL,
  plan_period                     TEXT NOT NULL,
  effective_period                TEXT,
  reported_at                     TIMESTAMPTZ,
  source_document_id              TEXT NOT NULL,
  beginning_aum_usd               DOUBLE PRECISION,
  ending_aum_usd                  DOUBLE PRECISION,
  employer_contributions_usd      DOUBLE PRECISION,
  employee_contributions_usd      DOUBLE PRECISION,
  benefit_payments_usd            DOUBLE PRECISION,
  refunds_usd                     DOUBLE PRECISION,
  net_external_cash_flow_usd      DOUBLE PRECISION,
  net_external_cash_flow_rate_pct DOUBLE PRECISION,
  consistency_gap_usd             DOUBLE PRECISION,
  disclosure_level                TEXT NOT NULL,
  evidence_refs                   JSONB
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
  confidence              DOUBLE PRECISION NOT NULL,
  evidence_refs           JSONB
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
  in_state_employee_universe  BOOLEAN NOT NULL DEFAULT FALSE,
  in_sampled_50               BOOLEAN NOT NULL DEFAULT FALSE
);

-- ── Canonical Entities ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS canonical_entities (
  stable_id             TEXT PRIMARY KEY,
  entity_type           TEXT NOT NULL,
  display_name          TEXT NOT NULL,
  normalized_name       TEXT NOT NULL,
  normalized_key_fields JSONB,
  merged_into           TEXT,
  created_at            TIMESTAMPTZ NOT NULL,
  updated_at            TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS entity_source_links (
  link_id               TEXT PRIMARY KEY,
  stable_entity_id      TEXT NOT NULL,
  source_record_id      TEXT NOT NULL,
  source_table          TEXT NOT NULL,
  evidence_refs         JSONB
);

-- ── Entity Lineage ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS entity_lineage_events (
  event_id              TEXT PRIMARY KEY,
  event_type            TEXT NOT NULL,
  source_entity_ids     JSONB,
  target_entity_ids     JSONB,
  occurred_at           TIMESTAMPTZ NOT NULL,
  actor                 TEXT NOT NULL,
  rationale             TEXT NOT NULL
);

-- ── API Keys ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS api_keys (
  key_id                TEXT PRIMARY KEY,
  key_hash              TEXT NOT NULL,
  hash_scheme           TEXT NOT NULL DEFAULT 'sha256',
  scopes                JSONB NOT NULL,
  status                TEXT NOT NULL DEFAULT 'active',
  created_at            TIMESTAMPTZ NOT NULL,
  label                 TEXT,
  revoked_at            TIMESTAMPTZ,
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
  fetched_at                TIMESTAMPTZ NOT NULL,
  mime_type                 TEXT NOT NULL,
  byte_size                 INTEGER NOT NULL,
  checksum_sha256           TEXT NOT NULL,
  is_active                 BOOLEAN NOT NULL DEFAULT TRUE,
  supersedes_artifact_id    TEXT,
  superseded_by_artifact_id TEXT,
  first_seen_at             TIMESTAMPTZ NOT NULL,
  last_seen_at              TIMESTAMPTZ NOT NULL
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
  confidence            DOUBLE PRECISION NOT NULL,
  routing_outcome       TEXT NOT NULL,
  priority              TEXT NOT NULL,
  state                 TEXT NOT NULL DEFAULT 'new',
  created_at            TIMESTAMPTZ NOT NULL,
  updated_at            TIMESTAMPTZ NOT NULL,
  evidence_refs         JSONB
);

-- ── Review Queue (Entities) ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS entity_review_queue (
  queue_id              TEXT PRIMARY KEY,
  candidate_id          TEXT NOT NULL,
  source_name           TEXT NOT NULL,
  entity_type           TEXT NOT NULL,
  plan_id               TEXT NOT NULL,
  plan_period           TEXT NOT NULL,
  candidate_entity_ids  JSONB,
  provenance_refs       JSONB,
  confidence            DOUBLE PRECISION,
  state                 TEXT NOT NULL DEFAULT 'new',
  resolution_action     TEXT,
  resolved_entity_ids   JSONB,
  created_at            TIMESTAMPTZ NOT NULL,
  updated_at            TIMESTAMPTZ NOT NULL
);

-- ── Discovered Inventory ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS discovered_inventory (
  inventory_id                    TEXT PRIMARY KEY,
  plan_id                         TEXT NOT NULL,
  plan_year                       INTEGER,
  document_type                   TEXT NOT NULL,
  source_url                      TEXT NOT NULL,
  source_authority_tier           TEXT NOT NULL,
  manager_disclosure_available    BOOLEAN NOT NULL DEFAULT FALSE,
  consultant_disclosure_available BOOLEAN NOT NULL DEFAULT FALSE
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
