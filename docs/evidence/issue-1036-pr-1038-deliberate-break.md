# Issue #1036 / PR #1038 deliberate-break transcript

Verification transcript on `gemini/issue-1036` for issue #1036 acceptance criteria.

## Provenance

- Branch: `gemini/issue-1036`
- Named test: `tests/ops/test_one_pdf_pilot.py::test_escalation_and_declared_authority_survive_to_bundle_and_api`
- Break target: `src/pension_data/export/workspace_bundle.py` (`build_workspace_bundle`)

## Baseline (HEAD)

```console
python3 -m pytest tests/ops/test_one_pdf_pilot.py::test_escalation_and_declared_authority_survive_to_bundle_and_api -q
python3 -m pytest tests/ops/test_one_pdf_pilot.py -q
```

```text
1 passed in 0.44s
12 passed in 0.53s
```

## Deliberate break: omit `review_context` in `build_workspace_bundle`

Temporarily omitted `review_context=resolved_review_context` when mapping staging rows in `src/pension_data/export/workspace_bundle.py::build_workspace_bundle`:

```python
    ui_rows = [map_staging_row_to_ui_row(row) for row in rows]
```

```console
python3 -m pytest tests/ops/test_one_pdf_pilot.py::test_escalation_and_declared_authority_survive_to_bundle_and_api -q -o addopts=
```

```text
F                                                                        [100%]
FAILED tests/ops/test_one_pdf_pilot.py::test_escalation_and_declared_authority_survive_to_bundle_and_api
AssertionError: assert ([{'actionable_flags': [], 'escalation_required': None, 'source_authority_status': 'unknown', 'source_authority_tier': None}, ...], [{'actionable_flags': [], 'escalation_required': None, 'source_authority_status': 'unknown', 'source_authority_tier': None}, ...]) == ([{'actionable_flags': ['doc_lineage_unreadable_pages:1', 'manual_review_needed'], 'escalation_required': True, 'source_authority_status': 'unknown', 'source_authority_tier': None}, ...], [{'actionable_flags': ['doc_lineage_unreadable_pages:1', 'manual_review_needed'], 'escalation_required': True, 'source_authority_status': 'unknown', 'source_authority_tier': None}, ...])
1 failed in 0.46s
```

`manifest_unknown.get("review_context") == expected_unknown_context` passed because `run_manifest.json` retained `review_context`, while both the direct bundle (`workspace-bundle.json`) and the rebuilt bundle (`scripts/web/build_workspace_bundle.py`) assertions failed.

## Revert and restore verification

Reverted the break in `src/pension_data/export/workspace_bundle.py::build_workspace_bundle` and re-ran:

```console
python3 -m pytest tests/ops/test_one_pdf_pilot.py::test_escalation_and_declared_authority_survive_to_bundle_and_api -q
python3 -m pytest tests/ops/test_one_pdf_pilot.py -q
```

```text
1 passed in 0.50s
12 passed in 0.53s
```

`scripts/check_deliberate_break.py` verification against `origin/main`:

```text
verdict: PASS (head-passed-base-failed)
```

## Producer validation follow-up (2026-11-17)

Extended the existing producer test across all `SOURCE_AUTHORITY_TIERS`, unknown
authority, and nullable/false/true escalation. Added checks for legacy unknowns,
missing context fields, runtime authority vocabulary, and invalid review contexts
in the shared builder/smoke/server validator. Four writer regression cases failed
before the fix: duplicate flags and whitespace-padded flags, authority status, and
authority tier were accepted and could overwrite a previously valid bundle. The
producer now rejects contexts that differ from their normalized values before
writing; all four cases pass and preserve the previous file bytes.

This sandbox denies local socket writes (`asyncio`'s wakeup socket raises
`PermissionError: [Errno 1] Operation not permitted`). Unmodified `TestClient` calls
hang, and Black's concurrent worker loop encounters the same restriction. These
verification commands used a temporary, uncommitted `sitecustomize.py` under
`/tmp/pension-data-test-runtime`, which caps selector waits at 10 ms. It changes
event-loop scheduling only; application code and test assertions are unchanged.
Black also used `BLACK_NUM_WORKERS=1` because the sandbox denies worker-process
socket creation.

```console
PYTHONPATH=/tmp/pension-data-test-runtime python3 -m pytest tests/ops/test_one_pdf_pilot.py::test_escalation_and_declared_authority_survive_to_bundle_and_api -q -m "not slow"
# 1 passed in 0.44s

PYTHONPATH=/tmp/pension-data-test-runtime python3 -m pytest tests/ops/test_one_pdf_pilot.py -q -m "not slow"
# 12 passed in 0.49s

PYTHONPATH=/tmp/pension-data-test-runtime python3 -m pytest tests/export/test_workspace_bundle.py tests/web/test_build_workspace_bundle.py tests/web/test_workspace_contract.py tests/api/test_artifact_data.py tests/api/test_app_serving.py tests/ops/test_one_pdf_pilot_cli.py tests/query/test_metric_history.py tests/renderer/test_shell_import_smoke.py -q -m "not slow"
# 208 passed in 3.94s

PYTHONPATH=/tmp/pension-data-test-runtime BLACK_NUM_WORKERS=1 black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
# 467 files would be left unchanged.

ruff check src/pension_data/export/workspace_bundle.py tests/export/test_workspace_bundle.py tests/web/test_workspace_contract.py
# All checks passed!

python3 scripts/web/sync_renderer_shell.py --check
# Renderer-shell materialization is current.
```

Replayed the same deliberate break on the follow-up working tree: temporarily
replaced the row mapping with `map_staging_row_to_ui_row(row)` while preserving
the manifest context. The named test failed at its combined direct/rebuilt bundle
assertion (`1 failed in 0.36s`); the diff showed both bundle row lists had empty
flags and null escalation. Restored the exact original source in a `finally`
block and reran the named test (`1 passed in 0.44s`).

Commit blocker: staging the verified source and tests failed with
`fatal: Unable to create '.git/index.lock': Read-only file system`. The sandbox
mounts `.git` read-only, so this run leaves its follow-up changes in the working
tree for a runner with Git write access to commit on the existing PR branch.

## Pilot and rebuild regression follow-up (2026-11-17)

Extended the existing pilot artifact test across every `SOURCE_AUTHORITY_TIERS`
value and absent authority, checking both the manifest and every direct bundle
row against the persisted parser result. Extended the existing environment and
CLI precedence tests to cover authority and added eight invalid declaration
cases, including rejection before falling back to a valid environment value.

Added twelve rebuild cases spanning every authority tier and nullable/false/true
escalation, plus four CLI rejection cases for null, partial, or conflicting
manifest contexts. Invalid inputs preserve the existing output file bytes.
Extended the existing multi-run API test to preserve distinct per-document review
contexts across run order, ingestion dates, and duplicate runs without changing
value resolution. The named acceptance test now checks every snapshot row.

The sandbox still denies socket writes. An unmodified named-test run timed out
after 20 seconds while waiting in asyncio's selector. Verification used the same
temporary selector-wait cap described above, now located at
`/tmp/pension-review-context-test-runtime/sitecustomize.py`; it is outside the
repository and changes no application behavior or assertions.

```console
PYTHONPATH=/tmp/pension-review-context-test-runtime python3 -m pytest tests/ops/test_one_pdf_pilot.py::test_escalation_and_declared_authority_survive_to_bundle_and_api -q -m "not slow"
# 1 passed in 0.44s (before the deliberate break)

# Temporarily omit review_context=resolved_review_context in build_workspace_bundle.
# The manifest equality assertion passes; the direct/rebuilt tuple assertion fails.
# Both bundle lists contain empty flags and null escalation instead of the manifest context.
# 1 failed in 0.44s

# Restore the exact source bytes in a finally block and rerun the named test.
# 1 passed in 0.54s

PYTHONPATH=/tmp/pension-review-context-test-runtime python3 -m pytest tests/ops/test_one_pdf_pilot.py -q -m "not slow"
# 23 passed in 0.78s (after restoring the source and formatting)

PYTHONPATH=/tmp/pension-review-context-test-runtime python3 -m pytest tests/export/test_workspace_bundle.py tests/web/test_build_workspace_bundle.py tests/web/test_workspace_contract.py tests/api/test_artifact_data.py tests/api/test_app_serving.py tests/ops/test_one_pdf_pilot_cli.py tests/query/test_metric_history.py tests/renderer/test_shell_import_smoke.py -q -m "not slow"
# 224 passed in 4.23s

PYTHONPATH=/tmp/pension-review-context-test-runtime BLACK_NUM_WORKERS=1 black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
# 467 files would be left unchanged.

ruff check tests/ops/test_one_pdf_pilot.py tests/web/test_build_workspace_bundle.py tests/api/test_artifact_data.py
# All checks passed!

python3 scripts/web/sync_renderer_shell.py --check
# Renderer-shell materialization is current.

git diff --check
# Passed.
```

The commit remains blocked: `git add` of the three changed Python test files
failed with `fatal: Unable to create '.git/index.lock': Read-only file system`.
No commit or push was made. These regression-test additions and this transcript
remain in the working tree for a runner with Git write access.

Verified functional checklist for this working tree:

- [x] Pilot validates declared authority and persists context in the manifest and direct bundle.
- [x] Bundle producer and runtime consumers carry and validate review context.
- [x] Rebuild script supplies manifest context for every contributing bundle row.
- [x] API preserves per-run/document attribution; renderer details and materialization checks pass.
- [x] Named escalation/authority/legacy acceptance test passes.
- [x] Deliberate break fails for direct and rebuilt bundles; restoring the source passes.
- [x] Whole pilot test file passes.
- [x] Optional metadata tuple includes authority; existing API multi-run tests are extended.
- [ ] Commit the regression-test additions on the existing PR branch (read-only `.git`).
- [ ] Publish this verification and update the PR checkboxes (`approval_policy=never`).

The GitHub connector rejected the PR-body checkbox update, verification comment,
and additive `needs-human` label with `MCP tool call requires approval, but
approval policy is never`. No remote metadata changes were made. PR #1038 was
confirmed open and ready for review (`draft=false`).

## Renderer follow-up and task reconciliation (2026-11-17)

Reviewed `81eb631`, `29e8389`, `ebbd91d`, and `a79d74c` before continuing.
Those commits contain the pilot, bundle, rebuild, API, renderer, and regression
changes described above; the earlier read-only Git notes describe previous runs,
whose changes have since been committed by the automation runner. The remote PR
checklist still has unchecked entries for verified work.

Extended the existing browser-script regression test across every declared
authority tier and unknown authority, crossed with nullable/false/true parser
escalation (12 cases). This checks escalation with unresolved authority, the
difference between false and unknown escalation, and simultaneous actionable
flags. Existing formatting, filtering, reset, row-click, legacy unknown, and
renderer materialization assertions remain intact.

Reconciliation verified against the current working tree:

- [x] Pilot validates optional declared authority and persists manifest/direct bundle context.
- [x] Bundle producer and runtime validators carry and validate context.
- [x] Rebuild script supplies manifest context to every bundle row.
- [x] API attaches context before combining runs; renderer displays it and materialization is current.
- [x] Named escalation/authority/API/legacy acceptance test passes.
- [x] Deliberate omission fails for direct and rebuilt bundles while the manifest retains context; restoring passes.
- [x] Whole pilot test file passes (23 tests).
- [x] Optional metadata tuple includes authority; existing API multi-run tests preserve per-document contexts.

Verification used the same temporary selector-wait cap documented above, at
`/tmp/pension-review-context-runtime/sitecustomize.py`. An unmodified HTTP test
stalled; a separate socketpair write reproduced `PermissionError: [Errno 1]
Operation not permitted`. No repository runtime or test assertions were patched
to work around that sandbox restriction.

```console
PYTHONPATH=/tmp/pension-review-context-runtime python3 -m pytest tests/ops/test_one_pdf_pilot.py::test_escalation_and_declared_authority_survive_to_bundle_and_api -q -m "not slow"
# 1 passed in 0.59s (baseline)

# Temporarily omit review_context=resolved_review_context in the bundle row mapping.
# Manifest equality passes, then the direct/rebuilt bundle tuple assertion fails:
# both bundle lists have null escalation and empty flags instead of manifest values.
# 1 failed in 0.55s
# Restore the exact original source bytes in a finally block and rerun:
# 1 passed in 0.52s

PYTHONPATH=/tmp/pension-review-context-runtime python3 -m pytest tests/ops/test_one_pdf_pilot.py tests/export/test_workspace_bundle.py tests/web/test_build_workspace_bundle.py tests/web/test_workspace_contract.py tests/api/test_artifact_data.py tests/api/test_app_serving.py tests/ops/test_one_pdf_pilot_cli.py tests/query/test_metric_history.py tests/renderer/test_shell_import_smoke.py -q -m "not slow"
# 258 passed in 5.26s

black --line-length 100 tests/web/test_workspace_contract.py
# 1 file left unchanged.
PYTHONPATH=/tmp/pension-review-context-runtime BLACK_NUM_WORKERS=1 black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
# 467 files would be left unchanged.
ruff check tests/web/test_workspace_contract.py
# All checks passed!
python3 scripts/web/sync_renderer_shell.py --check
# Renderer-shell materialization is current.
git diff --check
# Passed.
```

Handoff remains blocked by environment permissions:

- [ ] Commit the new Python renderer test and this evidence on the existing branch.
  `git add` fails creating `.git/index.lock`: `Read-only file system`.
- [ ] Update the PR checkboxes and publish the verification comment. Both the
  PR-body update and comment were rejected with `MCP tool call requires approval,
  but approval policy is never`; the additive `needs-human` label was also rejected.

No branch or PR was created, and no workflow or protected configuration changed.
