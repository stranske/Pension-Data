# Issue #961 / PR #966 deliberate-break transcript

## Mis-anchor note

Issue #961 Acceptance Criteria bullet 5 names
`src/pension_data/db/migrations/20260303_101_pg_core_fact_staging.sql` as the
file to disconnect. That migration is the **readback anchor** for JSONB /
TIMESTAMPTZ (unchanged by this PR). The Scope change under test is canonical
comparison in `src/pension_data/db/staging_persistence.py`.

A probe that rewrote `JSONB`/`TIMESTAMPTZ` to `TEXT` in the migration file
**without** dropping the live table left
`test_postgres_fetched_row_shape_requires_canonical_equality` **passing**,
because `CREATE TABLE IF NOT EXISTS` does not reshape an already-applied
schema. That probe is therefore hollow for this gate.

## Correct break

Temporarily replace `_same_assertion` / `_same_assertion_source` with raw `==`
(no `_canonical_evidence` / `_canonical_instant`).

### 1. Gate with canonical comparison disconnected (FAIL)

```text
$ PENSION_DATA_TEST_POSTGRES_URL="postgresql://postgres:postgres@localhost:5432/pension_data" \
  uv run pytest -q --no-cov --tb=line \
  tests/db/test_staging_persistence_postgres_integration.py::test_postgres_fetched_row_shape_requires_canonical_equality \
  tests/db/test_staging_persistence_postgres_integration.py::test_postgres_retry_is_idempotent

FF
FAILED ...::test_postgres_fetched_row_shape_requires_canonical_equality - AssertionError: assert False is True
FAILED ...::test_postgres_retry_is_idempotent - ValueError: conflicting assertion already exists for 'fact:pg:retry:1'
2 failed
```

### 2. Gate with canonical comparison restored (PASS)

```text
$ PENSION_DATA_TEST_POSTGRES_URL="postgresql://postgres:postgres@localhost:5432/pension_data" \
  uv run pytest -q --no-cov \
  tests/db/test_staging_persistence.py \
  tests/db/test_staging_persistence_postgres_integration.py

10 passed
```
