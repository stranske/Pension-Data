## Summary

Adds tests for the ranked anomaly, scenario, NL route, and NL chain targets. The current follow-up adds 36 parameterized cases for parameter binding, request validation before generation, provider usage precedence/conversion, persisted evidence normalization, blank provenance rejection, database error classification, fiscal-year ordering, and allocation alerts when funded ratios are missing.

Production code is unchanged; these tests found no real production bug.

## Reconciliation

Reviewed commits `531a001` and `4d77d50`: six changed files, not the reported 35. The latter added 95 artifact-loader/exporter cases outside issue #1045's ranked target list. Its verification report records 281 passing regression tests and deliberate-break failures for all 95 cases. Those additions do not complete any ranked target and their targeted gain cannot be added to the CI gain.

The original issue is now accessible through the GitHub connector. Selection returns to its ranking: anomaly gaps first, scenarios already at 100% in the focused run, then the next substantive gap in `nl_sql_chain.py`. The earlier off-list selection was based on unavailable issue details and should not be treated as authoritative ranking compliance.

## Coverage and remaining work

CI's supplied **89.95% whole-suite blended coverage** remains authoritative. The following measurements use only the three changed ranked test modules, always with `-m "not slow"`; pytest-xdist is not installed.

| Target | Before this follow-up | After | Local uncovered statements + branches |
| --- | ---: | ---: | ---: |
| `quality/anomaly_rules.py` | 96.74% | 97.83% | 6 → 4 |
| `quant/scenarios.py` | 100.00% | 100.00% | 0 → 0 |
| `api/routes/nl.py` | 95.83% | 95.83% | 2 → 2 |
| `langchain/nl_sql_chain.py` | 75.61% | 98.05% | 100 → 8 |

This follow-up covers **94 additional local units**, 86 short of 180 even before accounting for overlap with other CI tests. This is not a measured CI reduction: the issue's initial NL-chain gap was 85, whereas this focused baseline has 100. The exact whole-CI gain/shortfall remains unknown until Gate runs.

Anomaly coverage still has the numeric period-label fallback and a below-warning scoring path; detectors do not call that scoring path for sub-threshold shifts. The route retains its categorized artifact-selection gap. NL-chain gaps include lower-priority usage totals, typed list/tuple evidence rows, ambiguous-prompt error mapping, and a redundant interrupted-error mapping (execution converts interrupted SQLite errors to TimeoutError first). No direct private-helper tests were added to force those redundant paths.

Later ranked targets remain unverified in this partial delivery: `observability/langsmith_fleet.py` (27 issue units), `db/staging_persistence.py` (18), `extract/investment/security_positions.py` (18), and `query/saved_views/service.py` (7). They are not claimed untestable or complete. The delivery cannot establish the requested whole-CI reduction in this environment, and Git/GitHub write restrictions block delivery updates.

## Verification

Focused before/after command:

```sh
pytest tests/quality/test_anomaly_rules.py tests/quant/test_scenarios.py \
  tests/langchain/test_nl_sql_safety.py -q -m 'not slow' \
  --cov=pension_data.quality.anomaly_rules --cov=pension_data.quant.scenarios \
  --cov=pension_data.api.routes.nl --cov=pension_data.langchain.nl_sql_chain \
  --cov-report=term-missing
```

Results: **63 passed before; 99 passed after**, final combined target coverage **98.27%**.

After mutation restoration, the above modules plus `tests/api/test_artifact_data.py`, `tests/api/test_artifact_data_validation.py`, `tests/export/test_workspace_bundle.py`, `tests/export/test_workspace_bundle_validation.py`, `tests/query/test_metric_history.py`, and `tests/query/test_saved_views.py` pass together: **380 passed**, same 98.27% for the four ranked targets.

The full existing suite was attempted without excluding any test files:

```sh
pytest tests/ -q -m 'not slow' -o faulthandler_timeout=20 --no-cov
```

It stalls at `tests/api/test_app_serving.py::test_health_and_config_routes`; the dump shows Starlette TestClient waiting in AnyIO's `run_sync_from_thread`. The run was stopped after 45 seconds (hard-stop exit 137). Full-suite/CI acceptance stays unchecked.

## Deliberate breaks and restoration

Applied each mutation to the real production source and restored it in a finally block. JUnit results require every selected case to fail.

| Mutation | Cases | Failed |
| --- | ---: | ---: |
| Discard normalized request parameters | 3 | 3 |
| Replace request validation's domain error with ValueError | 7 | 7 |
| Replace extracted provider usage with a corrupt result | 12 | 12 |
| Discard row provenance | 7 | 7 |
| Corrupt deterministic error classification | 4 | 4 |
| Interpret two-digit fiscal years in the wrong century | 1 | 1 |
| Suppress all alerts when a funded ratio is missing | 2 | 2 |

All **36/36 new cases** failed under a deliberate break; all passed after restoration in the 380-test run. `git diff` confirms both production files are restored.

Removing only the missing-funded-ratio guard did not break the test because the subsequent finite-value guard also rejects None. The final mutation instead models the real regression this test prevents: dropping allocation alerts when funded data is missing. Proof for every parameterized case in the initial bootstrap commit has not been established; the all-PR mutation checkbox remains unchecked.

## Formatting

Both changed Python files were formatted with Black, line length 100. All 469 Python files passed individual checks to populate Black's verified cache, then the exact aggregate check passed (exit 0):

```sh
BLACK_CACHE_DIR=/tmp/pension-black-cache BLACK_NUM_WORKERS=1 \
  black --check --line-length 100 --exclude '(\\.workflows-lib|node_modules)' .
ruff check --output-format github --extend-exclude .workflows-lib .
git diff --check
```

The reusable CI formatter additionally excludes `.venv`; the required command above passes with the stricter requested exclusion. Ruff uses the same flags as CI's `lint-ruff` job.

## Delivery status

This file is the proposed replacement PR body because the GitHub update tool rejected the write: **“MCP tool call requires approval, but approval policy is never.”** It has not been applied remotely. Only evidence-supported checkboxes are reconciled below. The current PR is #1046 on `codex/issue-1045`; no branch or PR was created by this follow-up.

The required commit was attempted after formatting and lint passed, but staging
failed with `fatal: Unable to create '/home/runner/work/Pension-Data/Pension-Data/.git/index.lock': Read-only file system`.
Consequently this follow-up could neither commit nor push. The two changed test
files and this proposed body remain available for the automation's writer to
commit on the existing PR branch. No production mutation remains. The previous
iteration's edits are already committed as `4d77d50`; this restriction applies to
the current follow-up's 36 new cases.

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
1. [ ] Pick the next target by the repo's own coverage-gap ranking (escaped-defect priority, then
   churn, then uncovered mass), not by file size.
2. [ ] Every new test must FAIL when the code it covers is deliberately broken and PASS when
   reverted -- run that break-and-revert for real and report it, don't assert it.
3. [ ] Real bugs found while writing tests get fixed with a minimal, targeted diff in the same PR,
   not worked around. Watch for the negative-cache pattern (cache.get(key) is None used as a 'not yet cached' check, which can't tell 'never queried' from 'queried and got nothing') -- it has recurred independently four separate times fleet-wide already.
4. [x] Low blast radius only -- test-only or tightly-scoped fixes, no refactors.
5. [x] One PR per logical chunk of work.

- [ ] The goal is to improve the code, not a metric. If you measure this repo at or above 90% before
- [ ] starting, say so in a comment on this issue and close it without opening a PR -- do not
- [ ] manufacture a fix just because a round was requested.

<!-- auto-status-summary:end -->
