# Issue 1045: artifact validation coverage verification

This is a verified partial delivery on the existing PR branch. Production code is
unchanged. The tests exercise persisted artifact validation, temporal metadata,
review-context reconciliation, legacy evidence conversion, and workspace writer
validation before replacing an existing file.

## Baseline and target selection

The task supplies **89.95% blended line/branch coverage from the whole CI suite**.
That remains the authoritative baseline; the measurements below are targeted local
runs and must not be substituted for CI's per-file uncovered counts.

The supplied appendix contains no ranked file list, and `gh issue view 1045` and
`gh pr view` could not connect to `api.github.com`. Selection therefore follows
the available defect/churn evidence: `api/artifact_data.py` was changed by recent
review-context reconciliation and funding-trend fixes; `export/workspace_bundle.py`
was changed by the recent source-locator and review-context fixes. This is an
inferred ordering, not a recovered copy of the orchestrator's ranking.

Whole-suite verification was attempted with:

```sh
pytest -q -m 'not slow' -o faulthandler_timeout=30 --cov=src \
  --cov-report=json:/tmp/pension-coverage-before.json --cov-report=term:skip-covered
```

It collected 2,285 tests but stalled in the first test,
`tests/api/test_app_serving.py::test_health_and_config_routes`. The thread dump
shows the main thread waiting in AnyIO's `run_sync_from_thread` and the portal
thread waiting in the asyncio selector. The run was interrupted; no completed
whole-suite baseline was obtained. Existing tests were not edited or disabled.

## Targeted measurements

| Production target | Existing module tests | With new tests | Uncovered units before → after |
| --- | ---: | ---: | ---: |
| `api/artifact_data.py` | 82.31% | 96.28% | 95 → 20 |
| `export/workspace_bundle.py` | 72.31% | 100.00% | 85 → 0 |

Units are missing statements plus missing branches from coverage JSON. These
targeted runs cover **160 additional units**, 20 short of 180 even before any
overlap with other CI tests is accounted for. The actual whole-CI gain remains
unverified. The exporter has no remaining local gaps; some loader gaps are private
helper/default paths not reached by loading persisted artifacts. This delivery
does not add direct private-helper tests just to force those paths into the count.
Further target selection needs the missing ranked list and CI artifact.

The exporter baseline's tests all passed (37 tests), but its isolated coverage
was below the repository-wide 80% floor; that baseline command exited nonzero on
coverage alone. The final combined regression run passes the coverage floor:

```sh
pytest tests/api/test_artifact_data.py tests/api/test_artifact_data_validation.py \
  tests/export/test_workspace_bundle.py tests/export/test_workspace_bundle_validation.py \
  tests/query/test_metric_history.py tests/query/test_saved_views.py \
  -q -m 'not slow' --cov=pension_data.api.artifact_data \
  --cov=pension_data.export.workspace_bundle --cov-report=term-missing
```

Result: **281 passed**; the two targets together report **97.63%**. The 95 new
parameterized cases also passed separately before deliberate-break verification.

## Deliberate breaks and restoration

Each mutation was applied to the real production source, exercised with the new
tests, and restored before the next experiment. Validation-removal experiments
replace the module's domain-error raises with `pass`, so the tests must detect
either acceptance of malformed data or loss of the required error contract.

| Deliberate mutation | Selected cases | Failed as required |
| --- | ---: | ---: |
| Remove loader validation errors | 33 | 33 |
| Discard loaded `valid_to` | 1 | 1 |
| Merge duplicate review contexts by choosing the first | 8 | 8 |
| Discard loaded authority tier | 2 | 2 |
| Remove workspace validation errors | 38 | 38 |
| Discard normalized actionable flags | 1 | 1 |
| Remove the as-reported metric fallback | 10 | 10 |
| Remove document-map and default source-locator fallbacks | 2 | 2 |

JUnit node IDs confirmed that **all 95 new cases failed under a deliberate
break**. After restoring both production files, the combined 281-test run above
passed. `git diff` confirmed no production changes remained.

## Formatting and lint

Both new Python files were formatted with Black at line length 100. The exact
required aggregate check passed for all 469 Python files:

```sh
BLACK_CACHE_DIR=/tmp/pension-black-cache BLACK_NUM_WORKERS=1 \
  black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
ruff check --output-format github --extend-exclude .workflows-lib .
```

Black's default process pool cannot bind its forkserver socket in this sandbox,
and the one-worker asynchronous runner stalled. To complete the check, each file
was first checked individually with Black using that cache directory (469 checks,
zero failures). The normal aggregate command then passed using those verified
cache entries. Ruff and `git diff --check` also passed.

## Handoff

No acceptance checkbox for whole-suite CI, the 180-unit CI reduction, or the
authoritative ranking is marked complete. This round uses the checked-out branch
and creates no branch or PR, following the run's explicit delivery instruction.
GitHub connectivity must be restored to update the existing PR body with this
evidence and verify that the PR remains open and ready for review.

Staging the two test files and this report failed with:

```text
fatal: Unable to create '/home/runner/work/Pension-Data/Pension-Data/.git/index.lock': Read-only file system
```

Consequently this environment cannot create the requested commit or push it. The
verified edits remain in the working tree for a writer with access to `.git`.
No production mutation, branch, commit, or PR was left behind by the experiments.
