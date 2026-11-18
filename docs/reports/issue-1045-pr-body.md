## Summary

Adds discriminating tests for the eight coverage targets in issue #1045's order.
These follow-ups add 48 parameterized cases covering numeric period ordering,
categorized fleet artifact selection, usage-total precedence, decoded evidence,
ambiguous prompts, fleet trace/path/metadata handling, exact evidence persistence,
historical assertion retries, incomplete holdings, workbook cleanup, and saved-view
missing-data behavior, fleet retention with optional or failing file locking,
invalid evidence-key rollback, empty-batch transaction isolation, legacy retries,
and preservation of the last usable trace when an SDK response contains missing
or blank mapping references.
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
[Gate run 44263621600](https://github.com/stranske/Pension-Data/actions/runs/44263621600)
at PR head `1df1e953583f4980c0c7ac22f3ca44823fd7ee3c` passed on Python 3.13 and
3.14. The Python 3.14 coverage job reports **2,427 passed, 51 skipped; 92.48%**.
That result includes the previously committed tests; the two new trace-mapping
cases below still require CI on their eventual commit.

The ranked targets started with 186 uncovered statements plus branches according
to the issue. Their Gate output now leaves 11, a **175-unit reduction**, five
short of 180. This is the whole-CI comparison; focused gains are not added to it.
The overall 92.48% also includes the existing off-list artifact tests and other
suite coverage, so the entire percentage increase is not attributed to ranked tests.

| Ranked target | Issue's CI uncovered units | Latest Gate uncovered units | Reduction |
| --- | ---: | ---: | ---: |
| `quality/anomaly_rules.py` | 15 | 2 | 13 |
| `quant/scenarios.py` | 14 | 0 | 14 |
| `api/routes/nl.py` | 2 | 0 | 2 |
| `langchain/nl_sql_chain.py` | 85 | 2 | 83 |
| `observability/langsmith_fleet.py` | 27 | 3 | 24 |
| `db/staging_persistence.py` | 18 | 4 | 14 |
| `extract/investment/security_positions.py` | 18 | 0 | 18 |
| `query/saved_views/service.py` | 7 | 0 | 7 |
| **Total** | **186** | **11** | **175** |

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
added to this number to claim a whole-CI gain. The authoritative 175-unit CI
reduction and its five-unit shortfall are reported separately above.

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

Result after mutation restoration: **325 passed, 2 skipped; 99.50%** across the eight
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
as explained above. The latest Gate leaves one additional reachable fleet branch,
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
focused run gains zero; neither is claimed as a CI gain. The pending Gate will
determine whether its additional mapping branch is covered. This directly
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

## Delivery blocker

Staging the changed test and this prepared PR body failed with:

```text
fatal: Unable to create '/home/runner/work/Pension-Data/Pension-Data/.git/index.lock': Read-only file system
```

The environment exposes `.git` as read-only, so this run cannot commit or push.
The two verified files remain in the working tree for the automation writer to
commit on `codex/issue-1045`. No new branch or PR was created.

The remote PR-body/title update, blocker comment, and `needs-human` label were all
rejected with `MCP tool call requires approval, but approval policy is never`.
None of those remote changes was applied. This file is the prepared replacement
body; it requires a writer with Git and GitHub write access to finish delivery.

Closes #1045

<!-- auto-status-summary:start -->
## Verified status

- [x] Use the supplied whole-CI baseline; disclose differing focused measurements.
- [x] Run repository formatting and lint gates on the changes.
- [x] Follow the issue's ranking and explain unreachable remaining paths.
- [x] Run real break-and-revert checks for every case added by this follow-up.
- [x] Production code remains unchanged; keep the existing PR and branch.
- [x] Report the verified whole-CI reduction and explain the partial shortfall.
- [ ] Commit and push this round's verified changes and apply this body remotely.
- [ ] Whole-suite CI passes on the commit containing the two new cases.

<!-- auto-status-summary:end -->
