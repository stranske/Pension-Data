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

`test_legacy_active_retry_regression_detects_disabled_content_matching` now
reproduces the deliberate break automatically for SQLite and PostgreSQL. It
temporarily disables `_same_assertion_content`, invokes the named active-row
regression and requires its source-conflict error, then restores matching and
requires that same regression to pass. The patch is scoped to a monkeypatch
context, including when an assertion fails or PostgreSQL is unavailable.

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

Result after restoration and adding the automatic deliberate-break check:
**124 passed, 42 skipped**. All seven SQLite cases for the two acceptance tests
and the automatic deliberate-break check pass. The production source has no
remaining changes from this experiment.

The prior iteration recorded passing formatting and lint checks:

```sh
BLACK_NUM_WORKERS=1 BLACK_CACHE_DIR=/tmp/pension-black-cache \
  black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
ruff check tests/db/test_staging_persistence_postgres_integration.py
```

Black reports 467 files unchanged; Ruff reports all checks passed.

For this iteration, Black formatting of the changed test file and Ruff pass.
The required repository-wide Black CLI check stalled and was interrupted; a
second attempt timed out after 30 seconds. A serial check using Black's
`format_file_in_place` with `WriteBack.CHECK`, line length 100 and target Python
3.13 checked all 467 Python files and found none requiring formatting. The
exact CLI gate still needs to pass before committing. Git staging is also
blocked because `.git` is mounted read-only, so this iteration's source and
evidence changes remain uncommitted.

## PostgreSQL verification still required

Reviewed commits `a7c21a2`, `aae96b6`, and `3b9af26`: the PR body's three
implementation checkboxes already match the verified implementation. The
deliberate-break acceptance checkbox should now also be checked, bringing
verified progress to four of five tasks. The GitHub connector rejected the
PR-body update because it requires approval and this run's approval policy is
`never`; the remote checkbox remains unchanged.

PostgreSQL cases skipped because `PENSION_DATA_TEST_POSTGRES_URL` was unset.
PostgreSQL 18 binaries are available locally, but the required `psycopg` driver
is missing. Installing the project's existing PostgreSQL extra failed because
package network access is unavailable. The GitHub connector successfully
confirmed PR #1022 is open and ready for review, and listed no
pull-request-triggered PostgreSQL workflow run for head `3b9af26`. That query
does not include manually dispatched runs. GitHub CLI network access failed,
preventing inspection or dispatch of the existing PostgreSQL job.
Run that existing job and verify both named tests before completing the first
acceptance criterion.

## Local keepalive round 2 verification

Check date: 2026-11-09 (America/Chicago). Base head: `1c91b9a`.
Outcome: the two named regressions pass on SQLite; direct deliberate-break
verification fails as required and passes after restoration. PostgreSQL on
this revision remains pending the existing CI job.
Stop rule: three local acceptance attempts or 20 minutes. All three attempts
completed in under two minutes total; no additional local repair was needed.

Runtime: `/opt/anaconda3/bin/python3` (Python 3.12.2).

```sh
/opt/anaconda3/bin/python3 -m pytest \
  tests/db/test_staging_persistence_postgres_integration.py \
  -k '(legacy_keyed_active_row or superseded_legacy_replacement or legacy_active_retry_regression) and sqlite' -q
```

Restored result: **8 passed, 153 deselected in 0.20s**, no skips. This includes
all seven SQLite variants of the two named acceptance tests and the automatic
mutation check.

For the direct A2 experiment, temporarily changed only
`_same_assertion_content` to return `False`, then ran:

```sh
/opt/anaconda3/bin/python3 -m pytest \
  'tests/db/test_staging_persistence_postgres_integration.py::test_replacement_retry_matches_legacy_keyed_active_row[original-sqlite]' \
  -q --tb=short
```

Observed **1 failed in 0.25s**, raising
`ValueError: conflicting assertion source for 'audit:legacy-active:1'` at the
retry assertion. Restored the original file in a `finally` block, confirmed no
production diff, then obtained the eight passing cases above.

The initial dual-dialect selection reported 8 passed and 8 skipped because
`PENSION_DATA_TEST_POSTGRES_URL` is unset. CI run
[42731301950](https://github.com/stranske/Pension-Data/actions/runs/42731301950)
passed 153 tests without skips, but tested `a7c21a2`, before the latest source
and test changes. It does not establish A1 for this revision. The existing
`PostgreSQL Integration` workflow must verify the updated branch.
