## Summary

Adds discriminating tests for the eight coverage targets in issue #1045's order.
These follow-ups add discriminating cases covering numeric period ordering,
categorized fleet artifact selection, usage-total precedence, decoded evidence,
ambiguous prompts, fleet trace/path/metadata handling, exact evidence persistence,
historical assertion retries, incomplete holdings, workbook cleanup, and saved-view
missing-data behavior, fleet retention with optional or failing file locking,
invalid evidence-key rollback, empty-batch transaction isolation, legacy retries,
and preservation of the last usable trace when an SDK response contains missing
or blank mapping references, plus exact anomaly warning/critical thresholds, equal-threshold scoring, and
isolation of non-finite funded ratios and allocations in either observation,
and conservative confidence selection when one source has lower confidence.
The production changes fix two reproduced timestamp bugs. Sorting same-period observations
with mixed naive and timezone-aware timestamps raised `TypeError`. The sort key now
interprets naive timestamps as UTC, matching the existing evidence conversion.
Two new regression cases reproduce the crash before the fix and verify chronological
evidence and signed shift after it.
Two further regression cases reproduce reversed ordering across a daylight-saving
rollback. Aware timestamps are now converted to UTC before sorting, so both the
repeated wall-clock time and a rollback preserve chronological evidence and the
funded-ratio change's direction.

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
[Gate run 44280200490](https://github.com/stranske/Pension-Data/actions/runs/44280200490)
at PR head `74faee5505547a980b0582fd56e2ad050d1df5aa` passed on Python 3.13 and
3.14. The Python 3.14 coverage job reports **2,453 passed, 51 skipped; 92.49%**.
This was re-read from job `134718859694`; it includes both timestamp fixes,
their four regression cases, and the two finite plan-boundary cases. The four
new confidence-selection cases below still require CI after delivery.

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

Result after mutation restoration: **353 passed, 2 skipped; 99.50%** across the eight
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

## Corrupt-metric isolation follow-up

Reviewed commits `531a001` through `6812d39` and the live issue ranking before
implementation. The remote PR still has its bootstrap-only description and stale
checkboxes. Attempted to replace it with this reconciled report before adding
tests; the connector again rejected the update because approval policy is `never`.
The statuses below remain prepared local reconciliation, not remote changes.

The first ranked target remains the focus. Its earlier combined non-finite test
places the first corrupt observation in a different plan, so that observation
never participates in a comparison. Twelve new cases put NaN and both infinities
in either observation of a same-plan pair, separately for funded ratio and public
equity. Each case requires the other metric's material shift to retain its exact
score, severity, priority, direction, and period. This guards against both allowing
a corrupt value to poison scoring and suppressing valid anomalies on the same row.
No production bug was found and no production change remains.

The changed-module command above passes **36 tests before, 48 after; 98.91% in
both runs**, with the same one missing statement and one missing branch (line
150). This follow-up claims **zero new coverage units**; the authoritative
whole-CI ranked reduction remains **176, four short of 180**. The remaining
private/unreachable paths for each target are explained above.

Four deliberate source mutations were run and restored byte-for-byte in a
`finally` block. JUnit verified every selected case failed with exit 1:

| Deliberate mutation | Selected cases | Failed |
| --- | ---: | ---: |
| Abort instead of skipping a non-finite funded ratio | 6 | 6 |
| Abort instead of skipping a non-finite allocation | 6 | 6 |
| Remove the funded-ratio finite-value guard | 4 infinity cases | 4 |
| Remove the allocation finite-value guard | 4 infinity cases | 4 |

Every new case failed under a relevant break. After restoration the combined
eight-target command passes **343 tests, 2 skipped; 99.50%**. New tests have no
skips; the two pre-existing skips require optional openpyxl.

The whole-suite command was repeated without excluding any test files: **2,482
collected, one collection skip**, then the same first API TestClient test hung in
AnyIO. The 50-second bound exited 124. No test was disabled to avoid it. The
successful hosted Gate cited above covers the committed head; these twelve cases
remain pending CI. Both exact aggregate Black commands above passed (469 files),
as did the CI Ruff command and `git diff --check`. The first bounded Black run
stalled on worker shutdown; individual file checks populated Black's normal
cache before the successful aggregate commands.

## Mixed-timestamp regression and fix

The first unchecked task concerns reconciling whole-CI coverage with focused
measurements. The live issue ranking and latest committed-head Gate logs were
read before this change. The CI baseline remains 89.95%, and the CI ranked-target
reduction remains 176 units, four short of 180. Replacing the remote PR's stale
bootstrap body with this report was attempted, but rejected by the connector's
approval policy. Thus the live-body task is still blocked despite the verified
local reconciliation.

The first ranked target has no remaining meaningful public coverage gaps, as
explained above. Reviewing its timestamp contract found a real failure instead:
`detect_anomalies` normalized naive datetimes for evidence but compared them
against aware datetimes during same-period sorting. Both new regression cases
failed on the original source with `TypeError: can't compare offset-naive and
offset-aware datetimes`. They cover an aware previous observation and an aware
current observation, both using a nonzero UTC offset and reversed arrival order.
The minimal fix interprets naive sort timestamps as UTC while retaining the
aware datetime's actual instant. No input observation is changed.

The changed-module coverage command is:

```sh
pytest tests/quality/test_anomaly_rules.py -q -m 'not slow' \
  --cov=pension_data.quality.anomaly_rules --cov-report=term-missing
```

Before: **48 passed; 98.91%**. After: **50 passed; 98.91%**. The coverage table
still reports one missing statement and one missing branch at line 150. This
follow-up claims **zero additional coverage units**; the value is fixing the
reproduced crash rather than changing the metric.

For the required real break-and-revert, the fixed sort key was replaced with the
original `row.observed_at` comparison in the actual production source. Running
`pytest tests/quality/test_anomaly_rules.py -q -m 'not slow' -k mixed_timezones
--no-cov` returned exit 1: **2 failed, 48 deselected**. JUnit confirmed both
selected cases failed. The fixed file was restored byte-for-byte in a `finally`
block. The restored eight-target command above passes **345 tests, 2 skipped;
99.50%**. A targeted mypy check also passes for the changed production module.

The complete suite was attempted with no excluded test files:

```sh
timeout 50s pytest tests/ -q -m 'not slow' --no-cov -o faulthandler_timeout=20
```

It collected **2,484 tests, one collection skip**, then stalled in the existing
`tests/api/test_app_serving.py::test_health_and_config_routes` AnyIO/TestClient
wait. The bounded run exited 124. No existing test was edited, skipped, or disabled
to avoid the hang. Hosted Gate above covers the committed head, not this fix.

Both changed Python files were formatted with Black at line length 100. The two
exact aggregate Black commands recorded above then passed: **469 files unchanged**
for each command. Ruff's CI command, targeted mypy, and `git diff --check` passed.
The aggregate Black runner initially stalled; sequential single-file Black CLI
checks populated its normal cache before the successful aggregate checks.

## Daylight-saving timestamp regression and fix

The recent commits were reviewed before implementation. All eight ranked targets
already have public-boundary tests; their CI gaps and deliberate-break proofs are
reconciled above. The first unchecked live-PR task remains publication of that
whole-CI reconciliation. Updating the PR body with this prepared report was
attempted again and rejected: `MCP tool call requires approval, but approval
policy is never`. No remote task checkbox was changed.

The first ranked target still contained an observable ordering bug. Python
compares aware datetimes sharing the same timezone by their local wall time,
ignoring the DST fold. On the New York rollback, 01:45 in the first hour precedes
01:15 in the second hour in UTC, yet the detector reversed them. Identical 01:30
wall times also retained reversed arrival order despite distinct UTC instants.
Both new parameterized cases failed against the original source because previous
and current evidence timestamps were swapped; the signed funded-ratio change was
also reversed. The one-line fix converts aware sort timestamps to UTC. The tests
check chronological evidence, a -0.25 signed change, and unchanged input timezone
and fold metadata.

Focused verification:

```sh
pytest tests/quality/test_anomaly_rules.py -q -m 'not slow' \
  --cov=pension_data.quality.anomaly_rules --cov-report=term-missing
```

Before: **50 passed; 98.91%**. After: **52 passed; 98.91%**. The table still
reports 144 statements, one missing statement, 40 branches, and one missing branch
at line 150. This follow-up claims **zero additional coverage units**. The CI
ranked-target reduction remains **176 of about 180**, four units short; no focused
counts are added to it.

For the real break-and-revert, `else row.observed_at.astimezone(UTC)` was replaced
with the original `else row.observed_at` in the actual production file:

```sh
pytest tests/quality/test_anomaly_rules.py -q -m 'not slow' -k dst_fold \
  --no-cov --junitxml=/tmp/pension-dst-mutation.xml
```

Exit 1, **2 failed, 50 deselected**. JUnit confirmed both new cases failed, with
no errors or skips. The fixed source was restored byte-for-byte in a `finally`
block. The restored eight-target command above passes **347 tests, 2 skipped;
99.50%**. No production mutation remains.

The complete suite was attempted without exclusions:

```sh
timeout 50s pytest tests/ -q -m 'not slow' --no-cov -o faulthandler_timeout=20
```

It collected **2,486 tests, one collection skip**, then hung in the same existing
`tests/api/test_app_serving.py::test_health_and_config_routes` AnyIO
`run_sync_from_thread` wait. The bounded run exited 124. Existing tests were not
edited, skipped, or disabled. The successful hosted Gate above covers `b561289`,
not this new fix.

## Delivery blocker

The earlier mixed-timestamp fix and report are committed in `b561289`; automation
recovered the preceding run's read-only Git blocker. This run's daylight-saving
fix, two new regression cases, and report are prepared on `codex/issue-1045`.
No new branch or PR was created. The prepared task checklist below records local
verification; the live PR body still needs publication by a permitted writer.

Both changed Python files were formatted individually with Black at line length
100. Black's multi-file worker initially failed because the sandbox prohibits its
forkserver socket. Sequential single-file CLI checks populated Black's normal
cache; both exact aggregate Black commands above then passed, **469 files
unchanged** each. No formatter checks were bypassed. The current committed-head
Gate lint logs confirm the recorded CI flags. The CI Ruff command, targeted mypy,
and `git diff --check` also pass.

The commit attempt ran after those checks:

```sh
git add src/pension_data/quality/anomaly_rules.py \
  tests/quality/test_anomaly_rules.py docs/reports/issue-1045-pr-body.md
git commit -m 'fix: order anomaly observations by UTC across DST folds'
```

Staging failed with `fatal: Unable to create
'/home/runner/work/Pension-Data/Pension-Data/.git/index.lock': Read-only file system`.
The commit command was therefore not reached. The three changes remain available
for the automation writer; this run cannot commit or push with `.git` read-only.
The remote body update was also rejected with `MCP tool call requires approval,
but approval policy is never`. The blocker comment and `needs-human` label were
rejected for the same reason. PR #1046 was verified open with `draft=false` at
`b561289`; its live body and task checkboxes remain unchanged. Delivery remains
incomplete.

## Finite plan-boundary follow-up

Reviewed recent commits through `fe8eba1` and re-read the live issue ranking,
PR body, and latest committed-head Gate logs before implementation. The first
unchecked whole-CI reconciliation task remains blocked on the remote body update:
the connector rejected that write with `MCP tool call requires approval, but
approval policy is never`. The checked statuses in this file are a prepared
reconciliation, not a completed remote update. The original baseline remains
89.95%; the latest Gate still establishes a 176-unit ranked-target reduction,
four short of 180. No focused coverage gains are added to that figure.

The first ranked target has no remaining meaningful public coverage gaps. Its
existing cross-plan test combines plan boundaries with non-finite observations,
so two new finite-value cases independently protect the boundary guard. One
checks that single observations from different plans emit no anomaly; the other
checks that interleaved plan histories preserve each plan's real funded-ratio
change and evidence without creating a false cross-plan transition. Production
code is unchanged.

Changed-module coverage before: **52 passed; 98.91%**. After:
**54 passed; 98.91%**, using the anomaly coverage command recorded above.
Both leave line 150 and its branch uncovered. This follow-up claims **zero
additional coverage units** and improves the tests' ability to detect a defect.

For the actual break-and-revert proof, the production plan-boundary condition
was replaced with `if False`. The two new selected cases both failed with extra
cross-plan anomalies (**2 failed, 52 deselected**, exit 1); JUnit confirmed both
failures. The original production file was restored byte-for-byte in a `finally`
block. The restored eight-target command passes **349 tests, 2 skipped; 99.50%**.
The two new cases still need CI after delivery; no CI acceptance checkbox has
been checked on the strength of a local run.

Formatting and lint are verified: both exact aggregate Black commands recorded
above passed for all **469 files** after sequential single-file checks populated
Black's normal cache. The initial aggregate attempt failed because the sandbox
prohibits Black's forkserver socket. Ruff's CI command and `git diff --check`
passed. No formatter, test, coverage exclusion, or production source was changed.

The requested commit was attempted after those checks:

```sh
git add tests/quality/test_anomaly_rules.py docs/reports/issue-1045-pr-body.md
git commit -m 'test: prove anomaly comparisons stay within each plan'
```

Staging failed with `.git/index.lock: Read-only file system`, so no commit was
created and no push was possible. The two verified files remain available to the
automation writer. The PR body update, blocker comment, and `needs-human` label
were each rejected because connector writes require approval and approval policy
is `never`. Final PR inspection confirmed #1046 remains open with `draft=false`
at `fe8eba1`; its live body and task checkboxes remain unchanged. Delivery and
remote reconciliation remain incomplete.

## Conservative confidence follow-up

Re-read issue #1045's ranking and the whole-suite Gate logs at the checked-out
head `74faee5` before changing tests. The original CI baseline remains **89.95%**;
Gate run `44280200490`, job `134718859694`, reports **92.49%** after the PR's
existing work. Its ranked-target table still totals ten missing statements plus
branches, a **176-unit reduction**, four short of 180. This is an existing PR
continuation; the initial measurement preceded its coverage additions. No local
focused gain is added to the whole-CI count.

The first ranked target's remaining scorer path is filtered out by public anomaly
detection. Four new cases instead strengthen the existing confidence contract:
for warning and critical funded/allocation shifts, either the previous or current
observation can be the weaker source. Both anomaly types must retain that lower
confidence, its exact score, and its review-queue priority and reason. Earlier
critical tests put both sources on the same side of the priority threshold and
only bounded the score, so selecting the stronger source could pass them.
Production code is unchanged; no defect was found.

Changed-module coverage command:

```sh
pytest tests/quality/test_anomaly_rules.py -q -m 'not slow' \
  --cov=pension_data.quality.anomaly_rules --cov-report=term-missing
```

Before: **54 passed; 98.91%**. After: **58 passed; 98.91%**. Both JSON summaries
report one missing statement and one missing branch (line 150). This follow-up
claims **zero additional coverage units**, and does not mark the CI acceptance
criterion complete before the new tests are delivered and run there.

Actual break-and-revert runs independently replaced `min(previous.confidence,
current.confidence)` with `max(...)` in the funded and allocation detectors.
Each run selected `-k weaker_observation --no-cov -m 'not slow' --tb=short` and
failed all **four** cases (exit 1). JUnit confirmed every selected case failed
in both experiments. The production file was restored byte-for-byte in `finally`.
The restored eight-target command recorded above passes **353 tests, 2 skipped;
99.50%**. Both skips are existing optional-openpyxl cases.

A fresh full-suite attempt used the same bounded command recorded above:
`timeout 50s pytest tests/ -q -m 'not slow' --no-cov -o faulthandler_timeout=20`.
It collected **2,492 items / 1 skipped** and again stalled in the API serving
TestClient test. The trace shows AnyIO's `run_sync_from_thread` waiting on the
portal thread; the command exited **124**. No existing tests were skipped or
changed to avoid this environment limitation.

Black formatted the changed Python file at line length 100. After the first
bounded aggregate check timed out, sequential single-file checks populated the
normal writable cache. Both exact repository-wide Black checks recorded above
then passed (**469 files unchanged** each). Ruff's CI command and
`git diff --check` passed. No production diff remains.

Delivery was attempted after these checks:

```sh
git add tests/quality/test_anomaly_rules.py docs/reports/issue-1045-pr-body.md
git commit -m 'test: verify conservative anomaly confidence and triage'
```

Staging failed with `.git/index.lock: Read-only file system`; no commit was
created and no push was possible. The prepared PR body update, blocker comment,
and `needs-human` label each returned `MCP tool call requires approval, but
approval policy is never`. The final remote read confirmed PR #1046 is still
**open, draft=false**, at `74faee5`; its body and checkboxes remain unchanged.
The locally checked statuses below remain prepared reconciliation, not a claim
that those remote tasks were updated. The two verified files are ready for the
automation writer to commit and push; remote reconciliation remains blocked.

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

- [x] Same contract as every round in this initiative:
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
