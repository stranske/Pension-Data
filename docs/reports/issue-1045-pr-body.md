## Summary

Adds discriminating tests for the eight coverage targets in issue #1045's order.
These follow-ups add 46 parameterized cases covering numeric period ordering,
categorized fleet artifact selection, usage-total precedence, decoded evidence,
ambiguous prompts, fleet trace/path/metadata handling, exact evidence persistence,
historical assertion retries, incomplete holdings, workbook cleanup, and saved-view
missing-data behavior, fleet retention with optional or failing file locking,
invalid evidence-key rollback, empty-batch transaction isolation, and legacy retries.
Production code is unchanged.

The broader database suite establishes that historical retries without the stored
supersession timestamp are intentionally rejected. The new persistence test verifies
that contract, accepts an exact closed historical assertion, rejects changed history,
and checks that current and historical rows remain unchanged.

Earlier commit `4d77d50` added 95 artifact-loader/exporter cases outside the issue's
ranked target list. Those existing additions remain in the branch, but do not count
as ranking compliance or as a measured gain for the ranked targets. Their earlier
verification is recorded in `docs/reports/issue-1045-coverage-verification.md`.

## Coverage and partial delivery

**CI's supplied whole-suite blended baseline is 89.95%.** It remains authoritative.
The issue's whole-CI uncovered counts are reproduced below; local measurements are
focused runs, not substitutes for those counts. In particular, staging's local
baseline is 62 uncovered units versus CI's 18, and holdings' is 46 versus CI's 18.
Pytest-xdist is not installed, so these runs do not use `-n 4`.

| Ranked target | Issue's CI uncovered units | Before this follow-up | After | Local uncovered units before → after |
| --- | ---: | ---: | ---: | ---: |
| `quality/anomaly_rules.py` | 15 | 97.83% | 98.91% | 4 → 2 |
| `quant/scenarios.py` | 14 | 100.00% | 100.00% | 0 → 0 |
| `api/routes/nl.py` | 2 | 95.83% | 100.00% | 2 → 0 |
| `langchain/nl_sql_chain.py` | 85 | 98.05% | 99.51% | 8 → 2 |
| `observability/langsmith_fleet.py` | 27 | 94.18% | 99.57% | 27 → 2 |
| `db/staging_persistence.py` | 18 | 77.21% | 98.53% | 62 → 4 |
| `extract/investment/security_positions.py` | 18 | 78.50% | 100.00% | 46 → 0 |
| `query/saved_views/service.py` | 7 | 96.28% | 100.00% | 9 → 0 |

These follow-ups reduce **148 local uncovered statements plus branches**, 32 short
of 180 before allowing for overlap with other CI tests. The preceding follow-up's
94-unit focused reduction and the off-list tests' 160-unit reduction must not be
added to this number to claim a whole-CI gain. Gate has not established the actual
CI reduction or its shortfall. This is a verified partial delivery.

The focused runs leave these gaps:

- Anomaly detection: the private scorer's below-warning return, which the public
  detector filters out before scoring.
- NL chain: the private interrupted-error classification, which the public runner
  converts to TimeoutError before classification.
- Fleet records: private unsupported-operation validation. Public record generation
  uses a fixed operation list. No unsupported-operation test was added solely for
  coverage. Optional no-fcntl branches are now covered through public appending.
- Staging persistence: permissive canonicalization of legacy non-finite evidence
  and the temporal fallback for unsupported non-date/non-string values remain.
  No new test was added to endorse invalid JSON, and unsupported date objects
  cannot be bound by SQLite. Public-boundary tests now verify malformed-key
  rollback, legacy text/date retries, and empty-batch transaction isolation.
  The broader existing database suite passes.
- Scenarios, NL route, holdings, and saved-view service have no focused local gaps;
  that does not establish whole-CI completion.

## Verification

The before/after measurements use the same tests for each target group. The final
combined command is:

```sh
pytest tests/quality/test_anomaly_rules.py tests/quant/test_scenarios.py \
  tests/langchain/test_nl_sql_safety.py \
  tests/observability/test_langsmith_fleet.py \
  tests/observability/test_langsmith_fleet_status_derivation.py \
  tests/db/test_staging_persistence.py tests/db/test_bitemporal.py \
  tests/ingest/test_13f_parse.py tests/ingest/test_cafr_reconcile.py \
  tests/ingest/test_holdings_pipeline.py tests/query/test_saved_views.py \
  -q -m 'not slow' \
  --cov=pension_data.quality.anomaly_rules --cov=pension_data.quant.scenarios \
  --cov=pension_data.api.routes.nl --cov=pension_data.langchain.nl_sql_chain \
  --cov=pension_data.observability.langsmith_fleet \
  --cov=pension_data.db.staging_persistence \
  --cov=pension_data.extract.investment.security_positions \
  --cov=pension_data.query.saved_views.service --cov-report=term-missing
```

Result after mutation restoration: **323 passed, 2 skipped; 99.50%** across the eight
targets. Both skips are pre-existing real-workbook tests because optional openpyxl
is absent. The new workbook boundary tests use a fake reader and run without it.

A broader regression run also passes:

```sh
pytest tests/db/ tests/api/test_artifact_data.py \
  tests/api/test_artifact_data_validation.py tests/export/test_workspace_bundle.py \
  tests/export/test_workspace_bundle_validation.py tests/query/test_metric_history.py \
  -q -m 'not slow' --no-cov
```

Result: **377 passed, 48 skipped**, including all SQLite audit boundary tests.

The full suite was attempted without excluding test files:

```sh
pytest tests/ -q -m 'not slow' --no-cov -o faulthandler_timeout=20
```

It stalls in `tests/api/test_app_serving.py::test_health_and_config_routes`, with
Starlette TestClient waiting in AnyIO's `run_sync_from_thread`. The bounded run
was stopped after 50 seconds (exit 124), after collecting 2,458 tests and one skip.
Full-suite and CI acceptance stay unchecked;
no existing tests were disabled or edited to avoid this environment problem.

## Deliberate breaks and restoration

Each mutation changed the real production source and was restored in a finally
block. JUnit results confirmed every selected parameterized case failed. All 38
new cases from that iteration passed again after restoration in the focused command
above. The four additional optional-locking cases were also each broken and restored.

| Deliberate mutation | Selected cases | Failed |
| --- | ---: | ---: |
| Reverse numeric period order | 1 | 1 |
| Suppress categorized fleet artifact selection | 2 | 2 |
| Overwrite an earlier explicit usage total | 1 | 1 |
| Discard decoded list/tuple evidence | 2 | 2 |
| Misclassify ambiguous prompts | 2 | 2 |
| Omit default trace-client construction | 1 | 1 |
| Change default fleet artifact directory | 2 | 2 |
| Invent a missing response count | 2 | 2 |
| Corrupt persisted evidence serialization | 7 | 7 |
| Remove nested non-finite evidence rejection | 4 | 4 |
| Reject an exactly matching closed historical assertion | 1 | 1 |
| Convert invalid/missing holdings numbers to zero | 5 | 5 |
| Omit workbook close | 2 | 2 |
| Convert unknown cash flow to zero | 1 | 1 |
| Compute peer statistics for an empty peer set | 1 | 1 |
| Score unknown amortization as open | 2 | 2 |
| Remove the missing-subject guard | 1 | 1 |
| Convert whitespace CSV numbers to zero | 1 | 1 |
| Stop tolerating an unavailable fcntl import | 1 | 1 |
| Request unlock instead of the exclusive append lock | 1 | 1 |
| Stop tolerating lock acquisition OSError | 1 | 1 |
| Stop tolerating lock release OSError | 1 | 1 |

This round also closes the bootstrap proof gap: all **24 earlier cases** failed
under deliberate breaks (15 threshold validations, 6 scenario validations, empty
percentile input, non-finite anomaly suppression, and explicit artifact selection).
The prior follow-up recorded break-and-revert proof for its 36 cases; the off-list
report records proof for its 95 cases. No production mutation remains.

## Formatting and delivery

Changed Python files were formatted with Black at line length 100. All 469 Python
files passed both aggregate checks below. An initial bounded Black invocation
stalled in its worker runner; the subsequent aggregate checks used Black's normal
cache and both exited successfully. The current Workflows reusable CI source was
read to confirm its Black and Ruff flags; no workflow files were changed.

```sh
BLACK_CACHE_DIR=/tmp/pension-black-cache BLACK_NUM_WORKERS=1 \
  black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
BLACK_CACHE_DIR=/tmp/pension-black-cache BLACK_NUM_WORKERS=1 \
  black --check --line-length 100 --exclude '(\.venv|\.workflows-lib|node_modules)' .
ruff check --output-format github --extend-exclude .workflows-lib .
git diff --check
```

### Previous-round reconciliation and optional locking verification

Reviewed commits `531a001`, `4d77d50`, `454b672`, and `53b5984` before editing.
The live PR reports 13 changed files rather than the dispatch's 41-file warning.
All prior verified edits are committed at the live PR head `53b5984`; earlier
reports' statements that those edits were uncommitted are superseded by the
automation commits. The remote PR body still contains the bootstrap summary.
An attempt to reconcile its baseline checkbox, low-blast-radius checkbox, single-PR
checkbox, and evidence was rejected by the tool's approval policy before this
round's implementation. This file preserves the replacement body for a writer.

The first four targets' remaining focused gaps are either absent or private paths
unreachable from public callers, as explained above. The next meaningful gap in
the issue's order is fleet appending without Unix file locking, relevant to the
confirmed Windows target environment. Four new public-boundary cases check exact
NDJSON output, retention of existing records, temporary-file cleanup, and lock
calls when fcntl is unavailable, working, fails to acquire, or fails to release.
Each case failed under its own production-source mutation, then passed after
restoration; all production source remains unchanged.

```sh
pytest tests/observability/test_langsmith_fleet.py \
  tests/observability/test_langsmith_fleet_status_derivation.py \
  -q -m 'not slow' --cov=pension_data.observability.langsmith_fleet \
  --cov-report=term-missing
```

Before this round: **113 passed, 99.14%, 4 uncovered units**. After restoration:
**117 passed, 99.57%, 2 uncovered units**. Coverage JSON shows 321/322 statements
and 141/142 branches covered after this round. The only remaining statement and
branch are unsupported private operation validation. The combined 319-test result
above includes these four cases. No whole-CI reduction is inferred from this
two-unit focused gain, and no numeric acceptance checkbox is marked complete.

This run uses the existing `codex/issue-1045` branch and PR #1046. It creates no
branch or PR. Final delivery attempts were blocked:

- `git add` failed with `fatal: Unable to create
  '/home/runner/work/Pension-Data/Pension-Data/.git/index.lock': Read-only file system`.
  No commit or push was possible; the verified test edits remain for the automation's
  writer to commit on the existing branch.
- The GitHub body update was rejected with `MCP tool call requires approval, but
  approval policy is never`. This prepared body has not been applied remotely.
- The requested blocker comment and `needs-human` label were also rejected by the
  same approval policy; neither was applied.

The final read-only PR check confirms #1046 is **open** and **draft=false**.
No production changes or temporary mutations remain. A writer with Git and PR
write access must commit/push these tests and apply this prepared body.

### Current-round reconciliation and staging persistence verification

Reviewed commits `531a001`, `4d77d50`, `454b672`, `53b5984`, and `d47a864`.
All earlier additions are committed at `d47a864`; the dispatch's 34-file warning
does not match the actual 13-file PR diff. The live PR body remains the bootstrap
summary. Before implementation, an update to reconcile the baseline and verified
checkboxes was rejected with `MCP tool call requires approval, but approval policy
is never`. The baseline task is verified locally: this report uses CI's 89.95%
and discloses the differing local uncovered counts without claiming a CI gain.

The earlier ranked targets' remaining gaps are explained above. Four new cases
advance the next meaningful target, staging persistence, through its public API:

- Two invalid nested evidence-key cases reject the batch and roll back earlier rows.
- An empty batch preserves the caller's pending transaction rather than committing it.
- Retries normalize legacy text assertion dates, preserve original text evidence,
  and retain both assertions after a correction without creating retry revisions.

The focused before/after command is:

```sh
pytest tests/db/test_staging_persistence.py tests/db/test_bitemporal.py \
  -q -m 'not slow' --cov=pension_data.db.staging_persistence \
  --cov-report=term-missing --cov-report=json:/tmp/staging-after.json
```

Before: **23 passed, 95.59%, 12 uncovered units** (8 statements + 4 branches).
After restoration: **27 passed, 98.53%, 4 uncovered units** (2 statements + 2
branches). The eight-unit reduction is local; CI's supplied 18-unit gap remains
the authoritative starting figure for this target. No whole-CI reduction is known.

| Deliberate mutation | Selected cases | Failed |
| --- | ---: | ---: |
| Stringify invalid nested evidence keys instead of rejecting them | 2 | 2 |
| Commit the caller's transaction on an empty staging batch | 1 | 1 |
| Stop trimming legacy unparseable assertion dates during comparison | 1 | 1 |

Each mutation changed the production source, was run with pytest and JUnit case
verification, and was restored in a `finally` block. All four new cases failed;
the restored focused command passed. No production changes remain.

Broader regression: `pytest tests/db/ -q -m 'not slow'
--cov=pension_data.db.staging_persistence --cov-report=term-missing` gives
**138 passed, 48 skipped; 98.53%**. The ranked-target command above now gives
**323 passed, 2 skipped; 99.50%**.

The full suite was reattempted with a 45-second bound and
`-m 'not slow' --no-cov -o faulthandler_timeout=20`. It collected 2,462 tests and
one skip, then stalled in the first API TestClient test with the same AnyIO wait;
timeout exited 124. No test was disabled or changed to avoid that hang.

Black formatting of the changed file passed. Its default aggregate worker pool
could not bind a forkserver socket in this sandbox. A serial Black check of all
469 files (using Black's normal cache) passed, followed by both exact aggregate
Black commands above. Ruff's CI command and `git diff --check` passed.

Delivery is blocked by environment permissions: `git add` again failed creating
`.git/index.lock` because `.git` is read-only, preventing a commit or push. This
round's verified edits therefore remain in the working tree for the automation
writer. The live PR is open and ready for review (`draft=false`). This report
contains the prepared body and reconciliation; no remote checkbox update is claimed.

Closes #1045

<!-- auto-status-summary:start -->
## Verified status

- [x] Use the supplied whole-CI baseline; distinguish all focused measurements.
- [x] Run repository formatting and lint gates on the changes.
- [x] Keep this follow-up within the issue's ranked target list.
- [x] Run real break-and-revert checks for every case added by this follow-up.
- [x] Production code remains unchanged.
- [x] Keep the existing PR and branch.
- [ ] Commit and push verified changes and apply this evidence to the existing PR.
- [ ] Whole-suite pytest passes in CI.
- [ ] Gate establishes the whole-CI coverage reduction; partial shortfall is explained above.

<!-- auto-status-summary:end -->
