# Issue #1021 / PR #1022 legacy replacement verification

Verified on 2026-11-10 with Python 3.14.10.

- [x] Both legacy replacement tests pass on SQLite locally and PostgreSQL in the
  `postgres-integration` CI job. Round 11 rechecked the unchanged implementation
  and tests at `2464529531fbd243a329f9e706ee1bc915dc296b`: 20 focused SQLite cases
  passed without skips; PostgreSQL run `42754914292` passed all 197 cases without
  skips. See the round 11 receipt below for commands and final-head readback.
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

## Local keepalive round 5 verification

Check date: 2026-11-10. Python 3.14.10; base head: `df2dee7`.

Extended the named superseded-row regression to exercise successful batches in
both orders. An exact historical retry must allow an independent pending row
to persist, preserve every existing history column, and return zero when the
complete batch is retried in reverse order. The fetched pending row is compared
canonically so PostgreSQL JSONB and timestamp types participate in the check.

```sh
pytest tests/db/test_staging_persistence.py \
  tests/db/test_staging_persistence_postgres_integration.py \
  -m 'not slow' --no-cov -q
```

Result: **124 passed, 42 skipped**. All SQLite acceptance variants and the
deliberate-break check pass. PostgreSQL cases skip because the database URL is
unset. PostgreSQL 18 binaries are present, but `psycopg` is absent and PyPI DNS
resolution prevents installation. Ruff and `git diff --check` pass, and Black
leaves the edited test file unchanged.

The GitHub connector confirms PR #1022 is open with `draft=false`. Listing all
workflow runs for `df2dee7f41a8307e36e76b971e2ec09863586059` finds nine runs and
no PostgreSQL integration run. The PR targets `gemini/issue-1019`, whereas the
existing PostgreSQL workflow automatically runs only for PRs targeting `main`.
The updated revision still needs a dispatch of the existing PostgreSQL workflow
on `codex/issue-1021` after the test changes are committed. No workflow edits
are required. Acceptance remains unchecked pending that result.

Staging the test change fails with `Unable to create .git/index.lock: Read-only
file system`, so this run cannot create the required source commit locally.

The required repository-wide Black command first fails on sandbox socket
creation. A single-worker retry hangs; a bounded check using Black's standard
asyncio fallback reports **467 files would be left unchanged** when interrupted,
but exits 124. This is not a passing formatting gate. A writable run must
complete that gate before committing the Python change.

Attempts to add the `needs-human` label and post the blocker comment through
the GitHub connector were rejected: both tools require approval, but this
run's approval policy is `never`. The blocker report is therefore local only.

## Managed local keepalive verification (2026-11-09 America/Chicago)

Outcome: seven SQLite variants of the two named acceptance regressions and
one reversible legacy-content mutation check pass. The focused command below
prints the outcome in under two minutes. Stop rule: two attempts per command
or 20 minutes; stop at the first unresolved failure. Base head: `3b31d22`.

```sh
/opt/anaconda3/bin/python3 -m pytest -q \
  tests/db/test_staging_persistence_postgres_integration.py \
  -k 'replacement_retry_matches_legacy_keyed_active_row or audit_superseded_legacy_replacement_does_not_overwrite_newer_assertion or legacy_active_retry_regression_detects_disabled_content_matching'
```

Observed with Python 3.12.2: **8 passed, 8 skipped, 145 deselected in 0.38s**.
The PostgreSQL skips reflect an unset local database URL. The mutation test
replaces `_same_assertion_content` with a function returning false, requires
the named active-row regression to raise `conflicting assertion source`, then
restores matching and reruns that same regression successfully.

Blocking/drainable pair: local PostgreSQL is unavailable / dispatch the
existing `postgres-integration.yml` workflow on `codex/issue-1021` and inspect
its exact-head job log. The PR targets `gemini/issue-1019`, outside that
workflow's automatic `main` pull-request filter. Green Gate checks therefore
do not establish A1. No source, test, workflow, or dependency changes are
needed for this verification. PostgreSQL acceptance for the pushed revision
must be established from the dispatched run before reporting A1 complete.


## Managed local keepalive round 5 acceptance (2026-11-09 America/Chicago)

Outcome A1/A2: verified at base head `28e55dfcf4910f229689296915b90c1544bc48bf`.
The acceptance result is seven SQLite regression variants plus one reversible
mutation check passing locally, and both dialects passing in PostgreSQL CI.
Stop rule recorded before execution: three acceptance attempts or 20 minutes.
No production, test, workflow, or dependency-pin changes were needed.

```sh
/opt/anaconda3/bin/python3 -m pytest -q --no-cov \
  tests/db/test_staging_persistence_postgres_integration.py \
  -k '(replacement_retry_matches_legacy_keyed_active_row or audit_superseded_legacy_replacement_does_not_overwrite_newer_assertion or legacy_active_retry_regression_detects_disabled_content_matching) and sqlite'
```

Python 3.12.2 result: **8 passed, 153 deselected in 0.21s**, no skips.
The mutation check disables `_same_assertion_content`, requires the named
active-row retry to raise its source-conflict error, restores matching, and
requires the same named regression to pass.

[PostgreSQL CI run 42741656793](https://github.com/stranske/Pension-Data/actions/runs/42741656793)
reports head `28e55dfcf4910f229689296915b90c1544bc48bf`, successful job
`postgres-integration`, **163 passed in 1.37s**, no skips. Its log confirms
that the complete staging integration test file ran with the PostgreSQL URL
set. This verifies the round 4/5 test additions that the earlier notes left
pending. This evidence-only commit will receive another exact-head dispatch;
that run's outcome must be read before claiming acceptance on the pushed head.

Blocking/drainable pair: no local PostgreSQL URL / existing PostgreSQL CI
workflow dispatch on this branch. The automated keepalive continuation owns
the post-push review after its seven-minute floor; this round does not merge.

## Legacy-history isolation verification (2026-11-10)

Reviewed the commits through `269ceff`: all three implementation tasks and the
active-row deliberate-break check are implemented. The PR already has every
checkbox checked, but no PostgreSQL workflow run exists for `269ceff`; the last
recorded PostgreSQL success above predates its test changes. Updating the PR
body to reflect this pending acceptance was blocked by the connector's required
approval under this run's `never` approval policy. The PR is open and ready.

Added `test_legacy_history_lookup_ignores_other_fact_with_same_assertion_source`
for both dialects. Three cases check literal `%`, `_`, and backslash in fact IDs
against another fact's superseded legacy row with the same assertion source.
New restatements must insert once, retry without changes, and preserve the
other fact's full history. All raw SQL uses dialect-specific placeholders.

Temporarily passed an unescaped prefix only to the legacy-history query: the
SQLite percent and underscore cases both failed with the expected conflicting
assertion error. Restored the production source byte-for-byte. The restored
database suite (`python -m pytest tests/db -q --no-cov -m 'not slow'`) reports
**158 passed, 47 skipped**; skips are PostgreSQL cases because the local URL is
unset. The existing active-row deliberate-break regression also passes.

Ruff and `git diff --check` pass. The full Black check passes with all 467
Python files unchanged, using a temporary serial scheduler for Black's CLI to
avoid this sandbox's multiprocessing/thread scheduling failures. Formatting
options and Black's file selection and per-file checks remain unchanged.
## Finite validity follow-up (2026-11-10)

Reviewed recent commits through `38b2750` and confirmed that all three task
checkboxes already describe implemented code. Extended the named historical
legacy regression to exercise a non-null `valid_to`, including an equivalent
UTC-offset spelling on retry. Both dialects, batch orders, and document-source
variants are parametrized. The database suite reports **162 passed, 51 skipped**
with `pytest tests/db -m 'not slow' -q`; PostgreSQL skips because its URL is unset
and `psycopg` is absent. Ruff, `git diff --check`, and Black on the edited file pass.

The repository-wide Black gate fails on sandbox multiprocessing socket creation;
its single-worker retry stalls. No commit was created: `git add` independently
fails because `.git/index.lock` is on a read-only filesystem. Updating PR #1022
to leave current-head PostgreSQL acceptance pending was rejected by the GitHub
connector's approval requirement under policy `never`. The PR is open and ready.
PostgreSQL success recorded below predates the latest test additions; this
follow-up still needs a commit, the full formatting gate, and PostgreSQL CI.


## Local keepalive round 6 verification

Check date: 2026-11-09 (America/Chicago). Source/test head: `8f659e0`.
Outcome: **19 passed, 164 deselected in 0.56s**, zero skips. No production
or test changes were needed. This receipt is the only change in this round.
Stop rule: three failures at the same step or 20 minutes.

Command (runtime Python 3.12.2):

```sh
/opt/anaconda3/bin/python3 -m pytest -q --no-cov \
  tests/db/test_staging_persistence_postgres_integration.py -k 'legacy and sqlite'
```

This selection includes all five active-row variants, all eight historical-row
variants, the two retired-history variants, three literal fact-ID variants,
and the reversible deliberate-break check. The latter disables
`_same_assertion_content`, requires the named active retry test to raise
`ValueError: conflicting assertion source for 'audit:legacy-active:1'`, then
restores matching and reruns that same regression successfully.

A1 still requires the existing PostgreSQL Integration workflow on the pushed
head; earlier successful runs do not cover the latest source/test revision.
The automation continuation must inspect that exact-head job and review signals.

## Local keepalive round 7 acceptance verification

Check date: 2026-11-09 America/Chicago (2026-11-10 UTC).
Source/test head: `d01bf52a95684cb78eed14d7ef792a6196f29ba0`.
Outcome: **20 passed, 175 deselected in 0.48s**, zero skips, using the
owner-selected Python 3.12.2 runtime. Stop rule written before execution:
two attempts per failing acceptance step or 20 minutes.

```sh
/opt/anaconda3/bin/python3 -m pytest -q --no-cov \
  tests/db/test_staging_persistence_postgres_integration.py \
  -k '(replacement_retry_matches_legacy_keyed_active_row or audit_superseded_legacy_replacement_does_not_overwrite_newer_assertion or legacy_active_retry_regression_detects_disabled_content_matching) and sqlite'
```

The selection exercises ten active-row cases (base present and absent), eight
historical-row cases, and two reversible deliberate-break cases. The latter
disable legacy-content matching, require the named active-row test to raise
its source-conflict error, restore matching, and require that test to pass.
The broader focused `-k 'legacy and sqlite'` selection also passed all 25 cases
without skips in 0.62s. No production or test changes were needed.

Blocking/drainable pair: no local PostgreSQL connection is configured / the
existing PostgreSQL Integration workflow can be dispatched on this branch.
Its latest successful run, `42748296202`, tested `bd6bb93`, before this head's
test changes; it does not establish current A1. This receipt must be pushed
before dispatch so the job tests the actual delivered head. Exact-head CI
and post-push review remain required; green unrelated checks are insufficient.

## Local keepalive round 8 acceptance verification

Check date: 2026-11-09 America/Chicago (2026-11-10 UTC).
Verified source/test head: `f1348a8b79ff5fe1e0276bf4a3592e7df054e4d8`.
Stop rule: three acceptance attempts or 20 minutes, recorded before execution.
Required outcome: legacy retries preserve stored rows, conflicting historical
retries preserve the newer assertion, and disabling content matching breaks
the named active-row regression before restoration makes it pass.

```sh
/opt/anaconda3/bin/python3 -m pytest -q --no-cov \
  tests/db/test_staging_persistence_postgres_integration.py \
  -k '(replacement_retry_matches_legacy_keyed_active_row or audit_superseded_legacy_replacement_does_not_overwrite_newer_assertion or legacy_active_retry_regression_detects_disabled_content_matching) and sqlite'
```

Result: **20 passed, 175 deselected in 0.47s**, zero skips. The reversible
mutation cases require the named active-row test to raise the expected
source-conflict error with content matching disabled, then pass after restoration.

The previous round's PostgreSQL uncertainty is now resolved: workflow run
[`42750119924`](https://github.com/stranske/Pension-Data/actions/runs/42750119924)
used this exact head. Its `postgres-integration` job ran both integration files
with the configured PostgreSQL connection and reported **197 passed in 2.92s**,
zero skips. This includes both named regressions and the mutation cases on
PostgreSQL. The reported `tests_skipped` condition was not reproduced.

No production, test, workflow, or dependency changes were necessary. This
receipt changes documentation only. Blocking/drainable pair: final-head CI
is unverified until this receipt is pushed / dispatch the existing PostgreSQL
workflow on the pushed branch and read its exact-head result. Post-push review
remains a separate delivery check.

## Local keepalive round 9 acceptance verification

Check date: 2026-11-09 America/Chicago (2026-11-10 UTC).
Starting head: `f842805b23617ee8000d01abf6ebb132879430ec`.
Required outcome: exact legacy retries preserve stored rows, superseded retries
preserve the newer assertion, and disabling legacy content matching makes the
named active-row regression raise before restoring matching makes it pass.
Stop rule: two attempts per failing command or 20 minutes.

```sh
/opt/anaconda3/bin/python3 -m pytest -q --no-cov \
  tests/db/test_staging_persistence_postgres_integration.py \
  -k '(replacement_retry_matches_legacy_keyed_active_row or audit_superseded_legacy_replacement_does_not_overwrite_newer_assertion or legacy_active_retry_regression_detects_disabled_content_matching) and sqlite'
```

Observed result: **20 passed, 175 deselected in 0.51s**, zero skips. The mutation
cases execute the named regression with content matching disabled, require the
source-conflict exception, then execute it successfully after restoration.

The starting head's PostgreSQL run
[`42751597860`](https://github.com/stranske/Pension-Data/actions/runs/42751597860)
completed successfully: **197 passed in 3.03s**, zero skips, including both named
regressions and mutation cases. The `tests_skipped` condition did not reproduce.
No implementation, tests, workflow files, or dependency pins needed changes.

Blocking/drainable pair: this documentation commit needs final-head PostgreSQL
verification / dispatch `postgres-integration.yml` on `codex/issue-1021` and
read the run's head SHA and test summary. The current automation round leaves
the seven-minute post-push review to its next scheduled review pass.

## Local keepalive round 10 acceptance verification

Check date: 2026-11-09 America/Chicago (2026-11-10 UTC).
Starting head: `c7ddbed1e55a3e594209ddbd0fa06df49bfc3c80`.
Required outcome: exact legacy retries preserve stored rows, historical retries
preserve newer assertions, and disabling content matching breaks the active-row
regression before restoration makes it pass. Stop rule: three acceptance
attempts or 20 minutes, recorded before execution.

```sh
/opt/anaconda3/bin/python3 -m pytest -q --no-cov \
  tests/db/test_staging_persistence_postgres_integration.py \
  -k 'sqlite and (test_replacement_retry_matches_legacy_keyed_active_row or test_audit_superseded_legacy_replacement_does_not_overwrite_newer_assertion or test_legacy_active_retry_regression_detects_disabled_content_matching)'
```

Observed result: **20 passed, 175 deselected in 0.49s**, zero skips. The two
mutation cases disable matching, require the named active-row regression to
raise the expected source-conflict error, restore matching, and rerun it.
The starting head's PostgreSQL workflow
[`42753126058`](https://github.com/stranske/Pension-Data/actions/runs/42753126058),
job `129621165949`, reported **197 passed in 3.80s**, zero skips. The reported
`tests_skipped` failure was not reproduced; no implementation or test repair
was warranted. Dependency pins remain unchanged.

Blocking/drainable pair: the receipt's final head requires PostgreSQL CI /
dispatch the existing `postgres-integration.yml` on `codex/issue-1021` after
push and verify its head SHA and test summary. The scheduled continuation owns
the seven-minute post-push review. Merge remains separately blocked on the
prerequisite PR #1020; acceptance and review remain drainable on this branch.


## Local keepalive round 11 acceptance verification

Check date: 2026-11-09 America/Chicago (2026-11-10 UTC).
Starting head: `2464529531fbd243a329f9e706ee1bc915dc296b`.
Outcome 1: exact legacy retries preserve rows and older retries preserve newer
assertions; disabling matching produces the expected source conflict and
restoring matching makes the active-row regression pass. Stop rule recorded
before execution: two attempts per failing command or 20 minutes.

```sh
/opt/anaconda3/bin/python3 -m pytest -q --no-cov \
  tests/db/test_staging_persistence_postgres_integration.py \
  -k 'sqlite and (test_replacement_retry_matches_legacy_keyed_active_row or test_audit_superseded_legacy_replacement_does_not_overwrite_newer_assertion or test_legacy_active_retry_regression_detects_disabled_content_matching)'
```

Observed: **20 passed, 175 deselected in 0.43s**, zero skips. The automatic
mutation check disables `_same_assertion_content`, requires the named active-row
regression's source-conflict error, restores matching, then reruns it successfully.
Starting-head PostgreSQL workflow
[42754914292](https://github.com/stranske/Pension-Data/actions/runs/42754914292)
reported **197 passed in 3.87s**, zero skips. The reported `tests_skipped` failure
was not reproduced. No production, test, workflow, or dependency changes were
needed. Corrected the stale unchecked acceptance entry above.

Blocking/drainable pair: final-head PostgreSQL evidence pending this receipt's
push / dispatch the existing `postgres-integration.yml` on `codex/issue-1021`
and verify the resulting head SHA and test summary. The scheduled continuation
owns the seven-minute post-push review; this round does not merge the PR.


## Local keepalive round 12 acceptance verification

Check date: 2026-11-09 America/Chicago (2026-11-10 UTC).
Starting head: `c64d51f257595d092d13c7ff7d3729e78a2fa047`.
Required outcome: legacy retries are no-ops, historical retries preserve newer
assertions, and disabling content matching breaks the active-row regression
before restoration makes it pass. Stop rule recorded before execution: two
attempts per failing command or 20 minutes.

```sh
/opt/anaconda3/bin/python3 -m pytest \
  tests/db/test_staging_persistence_postgres_integration.py \
  -k '(replacement_retry_matches_legacy_keyed_active_row or audit_superseded_legacy_replacement_does_not_overwrite_newer_assertion or legacy_active_retry_regression_detects_disabled_content_matching) and sqlite' \
  --no-cov -q
```

Observed: **20 passed, 175 deselected in 0.44s**, zero skips, Python 3.12.2.
This includes both named acceptance regressions and the reversible mutation
check, which requires the expected source-conflict error with matching disabled
and passes the named regression again after restoring matching. No production,
test, workflow, or dependency change was warranted by this run.

Blocking/drainable pair: A1 needs PostgreSQL CI on the pushed receipt head /
dispatch `postgres-integration.yml` on `codex/issue-1021`, verify its head SHA,
and inspect the test summary for zero skips. This final automation round records
the push for the scheduled post-push review pass; it does not merge the PR.
