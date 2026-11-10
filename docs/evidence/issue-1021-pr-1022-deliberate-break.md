# Issue #1021 / PR #1022 legacy replacement verification

Verified on 2026-11-10 with Python 3.14.10.

- [ ] Both legacy replacement tests pass on SQLite locally and PostgreSQL in the
  `postgres-integration` CI job. SQLite passes; PostgreSQL CI remains unverified.
- [x] Disabling legacy-content matching makes the active-row retry test raise;
  restoring matching makes the test pass again.

## Regression cases

The active-row test now checks the original retry, JSON whitespace, UTC `Z`, an
equivalent nonzero offset, and explicit temporal defaults. It asserts that the
seeded legacy ID differs from the canonical replacement ID, and that repeated
retries preserve every stored column. Changes to the value, evidence, or
confidence still conflict and leave the database unchanged.

The superseded-row test also retries equivalent `asserted_at` and `superseded_at`
spellings while preserving history and the newest active assertion.

## Deliberate break

Temporarily replace the body of `_same_assertion_content` in
`src/pension_data/db/staging_persistence.py` with `return False`, disabling both
active and historical legacy-content matching. Run:

```sh
pytest tests/db/test_staging_persistence_postgres_integration.py \
  -k 'test_replacement_retry_matches_legacy_keyed_active_row and sqlite' \
  -m 'not slow' --no-cov --tb=line -q
```

All five selected variants fail at the retry with:

```text
ValueError: conflicting assertion source for 'audit:legacy-active:1'
5 failed, 154 deselected
```

Restore the original source before running the persistence suite:

```sh
pytest tests/db/test_staging_persistence.py \
  tests/db/test_staging_persistence_postgres_integration.py \
  -m 'not slow' --no-cov --tb=short -q
```

Result after restoration: **123 passed, 41 skipped**. All seven SQLite cases for
the two acceptance tests pass. The production source has no remaining changes
from this experiment.

Formatting and lint checks also pass:

```sh
BLACK_NUM_WORKERS=1 BLACK_CACHE_DIR=/tmp/pension-black-cache \
  black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
ruff check tests/db/test_staging_persistence_postgres_integration.py
```

Black reports 467 files unchanged; Ruff reports all checks passed.

## PostgreSQL verification still required

PostgreSQL cases skipped because `PENSION_DATA_TEST_POSTGRES_URL` was unset.
PostgreSQL 18 binaries are available locally, but the required `psycopg` driver
is missing. Installing the project's existing PostgreSQL extra failed because
PyPI DNS resolution is unavailable. GitHub API access also failed, preventing
inspection of the `postgres-integration` result or updating the PR checklist.
Run that existing job and verify both named tests before completing the first
acceptance criterion.
