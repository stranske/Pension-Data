# Core Fact Migrations

This folder stores SQL migration stubs for the bitemporal core fact schema.

- `20260302_001_core_fact_staging.sql`: creates staging tables for dual-reporting metric facts, cash-flow facts, and relationship tables plus strict curated views.
- `20260302_002_seed_backfill_compat.sql`: backfills legacy seed rows for `plan_period`, `benchmark_version`, and `ingestion_date` compatibility.
- `20260303_101_pg_core_fact_staging.sql`: PostgreSQL baseline schema for staging and curated metric view (JSONB evidence refs + TIMESTAMPTZ temporal columns).
- `20260303_102_pg_seed_backfill_compat.sql`: PostgreSQL-compatible seed backfill for `plan_period`, `benchmark_version`, and `ingestion_date`.
