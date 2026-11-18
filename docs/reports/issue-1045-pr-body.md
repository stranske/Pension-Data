## Summary

Adds discriminating tests for the eight coverage targets in issue #1045's order.
These follow-ups add 54 parameterized cases covering numeric period ordering,
categorized fleet artifact selection, usage-total precedence, decoded evidence,
ambiguous prompts, fleet trace/path/metadata handling, exact evidence persistence,
historical assertion retries, incomplete holdings, workbook cleanup, and saved-view
missing-data behavior, fleet retention with optional or failing file locking,
invalid evidence-key rollback, empty-batch transaction isolation, legacy retries,
and preservation of the last usable trace when an SDK response contains missing
or blank mapping references, plus exact anomaly warning/critical thresholds and equal-threshold scoring.
Production code is unchanged.

The broader database suite establishes that historical retries without the stored
supersession timestamp are intentionally rejected. The new persistence test verifies
that contract, accepts an exact closed historical assertion, rejects changed history,
and checks that current and historical rows remain unchanged.

Earlier commit `4d77d50` added 95 artifact-loader/exporter cases outside the issue's
ranked target list. Those existing additions remain in the branch, but do not count
as ranking compliance or as a measured gain for the ranked targets. Their earlier
verification is recorded in `docs/reports/issue-1045-coverage-verification.md`.

## Whole-CI baseline and verified result

The original issue baseline is **89.95% blended line/branch coverage**. The latest
[Gate run 44268406689](https://github.com/stranske/Pension-Data/actions/runs/44268406689)
at PR head `b34ac76e9c690ac9b6672ad0251f8a7f23cd6783` passed on Python 3.13 and
3.14. The Python 3.14 coverage job reports **2,429 passed, 51 skipped; 92.49%**.
That result includes all previously committed tests, including the trace-mapping cases; the six new anomaly boundary
cases below still require CI on their eventual commit.

The ranked targets started with 186 uncovered statements plus branches according
to the issue. Their Gate output now leaves 10, a **176-unit reduction**, four
short of 180. This is the whole-CI comparison; focused gains are not added to it.
The overall 92.49% also includes the existing off-list artifact tests and other
suite coverage, so the entire percentage increase is not attributed to ranked tests.

| Ranked target | Issue's CI uncovered units | Latest Gate uncovered units | Reduction |
| --- | ---: | ---: | ---: |
| `quality/anomaly_rules.py` | 15 | 2 | 13 |
| `quant/scenarios.py` | 14 | 0 | 14 |
| `api/routes/nl.py` | 2 | 0 | 2 |
| `langchain/nl_sql_chain.py` | 85 | 2 | 83 |
| `observability/langsmith_fleet.py` | 27 | 2 | 25 |
| `db/staging_persistence.py` | 18 | 4 | 14 |
| `extract/investment/security_positions.py` | 18 | 0 | 18 |
| `query/saved_views/service.py` | 7 | 0 | 7 |
| **Total** | **186** | **10** | **176** |

The initial task began below 90%; this measurement is after the existing PR's
changes. The existing PR is retained, with no new branch or PR.

## Focused measurements

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
added to this number to claim a whole-CI gain. The authoritative 176-unit CI
reduction and its four-unit shortfall are reported separately above.

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
  the latest Gate also reports zero gaps for these targets.

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

Result after mutation restoration: **331 passed, 2 skipped; 99.50%** across the eight
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
was stopped after 50 seconds (exit 124), after collecting 2,470 tests and one skip in this round.
This local environment limitation remains; the successful hosted Gate result
above supersedes earlier statements that whole-suite CI was unverified. No existing
tests were disabled or edited to avoid the hang.

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
| Stringify invalid nested evidence keys instead of rejecting them | 2 | 2 |
| Commit the caller's transaction on an empty staging batch | 1 | 1 |
| Stop trimming legacy unparseable assertion dates during comparison | 1 | 1 |
| Invent a trace reference after exhausting mapping keys | 2 | 2 |

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

## Trace-mapping follow-up

The first four ranked targets have no remaining meaningful public paths to cover,
as explained above. The preceding Gate at `1df1e95` left one additional reachable fleet branch,
`492->497`: exhausting a mapping's candidate reference keys. Two new cases call
`LangSmithClientTraceSink.emit` with a scripted client response sequence. After a
valid trace, an empty mapping or a mapping of blank references must preserve the
last usable trace ID and URL. No production code changes were necessary.

The changed-test-module command is:

```sh
pytest tests/observability/test_langsmith_fleet.py -q -m 'not slow' \
  --cov=pension_data.observability.langsmith_fleet --cov-report=term-missing
```

Before: **37 passed, 92.24%, 36 uncovered units**. After: **39 passed, 92.89%,
33 uncovered units**. Including the adjacent status-derivation tests gives
**117 → 119 passed; 99.57% → 99.57%**, with two local uncovered units in both runs.
The changed-module run covers three additional local units, while the broader
focused run gains zero; neither focused count is claimed as a CI gain. Gate `44268406689` now
confirms that the mapping gap closed: fleet has two uncovered units, down from three. This directly
illustrates why the issue's CI counts must not be replaced with focused counts.

For break-and-revert, the actual production `_resolve_run_attr` fallback was
changed from `None` to `"missing-trace"`. Both selected cases failed at the final
trace-ID assertion (**2 failed, 37 deselected**, exit 1), as confirmed by JUnit.
The production file was restored byte-for-byte in a `finally` block; the restored
ranked-target run above passed (**325 passed, 2 skipped**).

The first bounded aggregate Black check stalled on worker shutdown (exit 124).
Subsequent runs of both exact aggregate Black commands above passed: **469 files
unchanged**. Ruff's CI command and `git diff --check` passed. No workflow,
configuration, coverage threshold, exclusion, or production source was changed.

## Reconciliation and anomaly threshold follow-up

Reviewed commits `531a001` through `b34ac76` before this follow-up. The previous
iterations' test and report changes are committed in `b34ac76` and its ancestors;
the earlier delivery failure was recovered by the automation writer. PR #1046 is
open and ready (`draft=false`). The actual remote body still describes only the
bootstrap tests; its checkbox reconciliation was attempted before implementation
and rejected because the connector requires approval while approval policy is
`never`. The checked statuses below describe verified local evidence, not a
successful remote update.

The first ranked target's remaining two uncovered units are the private scorer's
below-warning floor, unreachable through the public detector. No test was added
to invoke that dead path solely to change coverage. Six new public-path cases
instead guard exact warning and critical boundaries for funded ratios and asset
allocations, equal warning/critical thresholds (no division by zero), and exact
confidence-cutoff priorities through review-queue routing. These are regression
checks for the first ranked target's escaped-defect risk; no production bug was
found and no production source change remains.

The required changed-module command was run before and after:

```sh
pytest tests/quality/test_anomaly_rules.py -q -m 'not slow' \
  --cov=pension_data.quality.anomaly_rules --cov-report=term-missing
```

Before: **30 passed; 98.91%**. After: **36 passed; 98.91%**. Both leave one
statement and one branch uncovered. This round claims **zero new coverage units**;
the authoritative committed-head reduction remains 176, four short of 180.
Private/unreachable remaining paths for every ranked target are explained above.

Four real source mutations were exercised, each restored byte-for-byte in a
`finally` block. JUnit confirmed the selected counts and failures:

| Deliberate mutation | Selected cases | Failed |
| --- | ---: | ---: |
| Make warning comparison exclusive | 2 | 2 |
| Make critical comparison exclusive | 2 | 2 |
| Remove equal-threshold span protection | 2 | 2 |
| Make the confidence cutoff exclusive | 6 | 6 |

Every new case failed under a relevant deliberate break. After restoration, the
combined eight-target command above passes **331 tests, 2 skipped; 99.50%**.
The whole suite was attempted without excluding any test files: 2,470 collected,
one skip, then a hang in the first API TestClient test; the 50-second bound exited
124. The hosted Gate evidence above applies to `b34ac76`, before these six cases.

## Delivery blocker

Staging the changed test and this prepared PR body failed with:

```text
fatal: Unable to create '/home/runner/work/Pension-Data/Pension-Data/.git/index.lock': Read-only file system
```

The environment exposes `.git` as read-only, so this run cannot commit or push.
The changed test and this reconciled body remain in the working tree for the automation writer to
commit on `codex/issue-1045`. No new branch or PR was created.

The remote PR-body/title update, blocker comment, and `needs-human` label were all
rejected with `MCP tool call requires approval, but approval policy is never`.
None of those remote changes was applied. This file is the prepared replacement
body; it requires a writer with Git and GitHub write access to finish delivery.

Closes #1045

<!-- auto-status-summary:start -->
## Automated Status Summary
#### Scope
Pension-Data reads 89.95% blended line+branch in CI; reaching 90% needs about 100 more covered statements+branches. The target order below is the Orchestrator's own ranking (escaped defects, then churn, then uncovered mass -- unchanged). Only the bundling is new: earlier rounds spent a whole PR on files with a handful of uncovered statements, so one PR now covers the next ranked targets until it has covered about 180 units.

#### Tasks
- [x] "Uncovered units" here come from the repository's whole CI run (every test file together). One test module's own suite can already cover more than CI's count shows for that module, and CI's figure is the one this PR is judged on: if your baseline differs, say so in the PR body and use CI's number.
- [x] Before you push, run the repository's own formatter and linter on every file you add or change, with the flags its CI `lint-format` and `lint-ruff` jobs use: one unformatted file fails a required check, and waiting for the autofix bot to repair it afterwards has not worked on lane PRs.
- [ ] Deliver what you verified. If you cannot reach the goal for ANY reason (a broken local environment, a target you could not finish, the time you had), still open the pull request with the verified work you have, say in its body how far short it is and why, and never leave verified commits or edits unpushed or uncommitted: a partial pull request that merges is worth more than a complete one that never opens, and a worktree left holding work blocks this issue's next dispatch.

#### Acceptance criteria
- [ ] `pytest` passes in CI with the new tests, and each new test fails when the code it covers is deliberately broken (the PR body shows the break-and-revert run).
- [ ] The PR's Gate coverage shows the targets' uncovered statements+branches down by about 180 or more, or the PR body states how far short it is and why and explains each target that could not be meaningfully covered (a partial PR meets this criterion; no PR does not).
- [x] Production code changes only where a test found a real bug, each one explained in the PR body.

- [ ] Same contract as every round in this initiative:
1. [x] Pick the next target by the repo's own coverage-gap ranking (escaped-defect priority, then
   churn, then uncovered mass), not by file size.
2. [x] Every new test must FAIL when the code it covers is deliberately broken and PASS when
   reverted -- run that break-and-revert for real and report it, don't assert it.
3. [x] Real bugs found while writing tests get fixed with a minimal, targeted diff in the same PR,
   not worked around. Watch for the negative-cache pattern (cache.get(key) is None used as a 'not yet cached' check, which can't tell 'never queried' from 'queried and got nothing') -- it has recurred independently four separate times fleet-wide already.
4. [x] Low blast radius only -- test-only or tightly-scoped fixes, no refactors.
5. [x] One PR per logical chunk of work.

- [ ] The goal is to improve the code, not a metric. If you measure this repo at or above 90% before
- [ ] starting, say so in a comment on this issue and close it without opening a PR -- do not
- [ ] manufacture a fix just because a round was requested.

<!-- auto-status-summary:end -->
