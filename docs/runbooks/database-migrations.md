# Database Migrations Runbook

## Purpose

Apply schema migrations idempotently for local SQLite and production PostgreSQL environments.

## Migration State Contract

Migrations are tracked in `schema_migrations`:

- `version` (primary key): migration version derived from SQL filename stem.
- `applied_at`: timestamp recorded when migration is applied.

Reruns skip previously recorded versions.

## Local SQLite

```bash
uv run python scripts/run_db_migrations.py \
  --environment local \
  --database-url "sqlite:///./.pension-data/pension_data.db"
```

Expected behavior:

- Creates SQLite DB path if missing.
- Applies pending migrations under `src/pension_data/db/migrations/`.
- Outputs applied and skipped versions as JSON.

## Production PostgreSQL

```bash
uv run python scripts/run_db_migrations.py \
  --environment production \
  --database-url "postgresql://<user>:<pass>@<host>:5432/<db>"
```

Expected behavior:

- Requires `psycopg` client dependency (`uv sync --extra postgres --dev`).
- Creates `schema_migrations` if missing.
- Applies pending PostgreSQL migrations in deterministic filename order.

## Rollback Expectations

Current migration approach is forward-only:

1. Restore from DB backup/snapshot if a migration must be reverted.
2. Add a compensating forward migration for controlled remediation.
3. Re-run migration command to restore convergent schema state.

## CI Coverage

`.github/workflows/postgres-integration.yml` provisions a PostgreSQL service, applies migrations, and runs real-DB integration tests for:

- SQL endpoint pagination + timeout behavior.
- Staged core metric persistence roundtrip.
