# Issue #1021 / PR #1022 legacy replacement verification

Verified on 2026-11-10 with Python 3.14.10.

- [ ] Both legacy replacement tests pass on SQLite locally and PostgreSQL in the
  `postgres-integration` CI job. Round 3 verified an earlier revision; the test
  changes in round 4 still require PostgreSQL CI verification.
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

## Local keepalive round 3 verification

Check date: 2026-11-09 (America/Chicago). Tested head: `2c1962f`.
Outcome A1: both named regressions pass locally on SQLite and in the existing
`postgres-integration` CI job on this exact head. No production or test changes
were necessary. This round changes only this evidence file.

Acceptance command (also exercises the A2 deliberate-break check):

```sh
/opt/anaconda3/bin/python3 -m pytest -q \
  tests/db/test_staging_persistence_postgres_integration.py \
  -k 'replacement_retry_matches_legacy_keyed_active_row or audit_superseded_legacy_replacement_does_not_overwrite_newer_assertion or legacy_active_retry_regression_detects_disabled_content_matching'
```

Result: **8 passed, 8 skipped, 145 deselected in 0.19s**. The eight skips
are PostgreSQL variants because the local database URL is unset; the seven
named SQLite acceptance variants and the A2 mutation check all pass.

Inspected [CI run 42736444509](https://github.com/stranske/Pension-Data/actions/runs/42736444509)
and its job log: head SHA `2c1962fbca2123b998c2a25c3230a5013fe4e8ee`,
job `postgres-integration`, **163 passed in 1.55s**, no skips. The command
includes this entire staging integration test file and sets the PostgreSQL
database URL. Thus the named PostgreSQL variants actually ran successfully.

Stop rule written before execution: two attempts per acceptance command or
15 minutes. The local command passed on its first attempt. No dependency pins
changed, no full local suite ran, and no new PR was opened.

## Local keepalive round 4 verification

Check date: 2026-11-10. Python 3.14.10; base head: `34e316a`.

Strengthened the named active-row regression to test conflicts with an
independent pending insert in both batch orders. Full-column snapshots now
include both plans, so a failed batch must preserve legacy rows and roll back
the pending insert. A successful mixed batch must insert the pending row after
the legacy no-op, preserve the legacy rows, and remain idempotent on retry.
All new snapshot parameters use the existing dialect-specific placeholders.

```sh
pytest tests/db/test_staging_persistence.py \
  tests/db/test_staging_persistence_postgres_integration.py -m 'not slow' -q
```

Result: **124 passed, 42 skipped**. The named SQLite regressions and the existing
deliberate-break check passed. PostgreSQL variants skipped because
`PENSION_DATA_TEST_POSTGRES_URL` is unset. PostgreSQL 18 binaries are available,
but `psycopg` is absent and installation failed because PyPI DNS resolution is
unavailable. GitHub API access also failed, so this round cannot establish
PostgreSQL CI success or inspect PR readiness. The existing CI job already
includes both named regressions; no workflow changes are required.

An isolated connection wrapper that makes `rollback()` a no-op caused the
strengthened active-row regression to fail at its snapshot assertion, confirming
that it detects a pending insert surviving a failed batch. The wrapper was
temporary and did not change repository files. Ruff and `git diff --check`
passed. Black left the edited test file unchanged.

The required repository-wide Black check failed on sandbox socket creation.
With `BLACK_NUM_WORKERS=1`, a bounded retry reported **467 files would be left
unchanged** when interrupted at 45 seconds, but exited 124. This is not a
successful formatting gate; a writable, unrestricted run must complete it
before committing.

Committing is blocked: `git add` cannot create `.git/index.lock` because `.git`
is mounted read-only. Changes remain in the working tree for a writable run.
