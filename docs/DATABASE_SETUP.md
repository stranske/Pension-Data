# Database Setup

Last reviewed: 2026-03-03

This repo uses a split DB strategy:

- Local/dev/test: SQLite (`sqlite:///...`)
- Production/shared: PostgreSQL (`postgresql://...`)

## Local (SQLite)

Default URL:

```text
sqlite:///./.pension-data/pension_data.db
```

Notes:

- No DB server is required.
- The SQLite file is created automatically.
- For isolated tests, use `sqlite:///:memory:`.

## Production (PostgreSQL)

Requirements:

- PostgreSQL 15+
- Install postgres client dependency:

```bash
pip install -e ".[postgres]"
```

- Provide a DSN:

```text
postgresql://<user>:<password>@<host>:5432/<database>
```

## Migration Sequences

SQLite migrations:

1. `src/pension_data/db/migrations/20260302_001_core_fact_staging.sql`
2. `src/pension_data/db/migrations/20260302_002_seed_backfill_compat.sql`

PostgreSQL migrations:

1. `src/pension_data/db/migrations/20260303_101_pg_core_fact_staging.sql`
2. `src/pension_data/db/migrations/20260303_102_pg_seed_backfill_compat.sql`

## Runtime Strategy Module

Use `src/pension_data/db/strategy.py` to:

- Resolve DB config from environment + URL.
- Select migration sequence by dialect.
- Open SQLite/PostgreSQL connections with consistent guardrails.
