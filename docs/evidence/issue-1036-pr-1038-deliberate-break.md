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

## Same-document revision attribution follow-up (2026-11-17)

Reviewed all six PR commits through `470ff36` and reran their relevant checks
before continuing. The existing same-document revision test now verifies distinct
review contexts for two runs of the same document: the original is escalated with
unknown authority (or is a legacy artifact), and the revision explicitly clears
escalation and declares `official-mirror` authority. Both run-directory orders
are tested. Assertions preserve each ingestion date's value and context while
retaining the existing check that the latest revision supplies the trend value.

Reconciled functional checklist, verified on this working tree:

- [x] Pilot validates optional authority and persists manifest/direct bundle context.
- [x] Producer and runtime contracts validate bundle review context.
- [x] Rebuilding supplies manifest context for each bundle row.
- [x] API preserves run/document context; renderer materialization is current.
- [x] Named acceptance test passes, including legacy and simultaneous conditions.
- [x] Omitting context propagation fails the direct/rebuilt assertion; restoring passes.
- [x] Whole pilot test file passes (23 tests).
- [x] Required metadata tuple and existing API multi-run/materialization tests are preserved.

Replayed the deliberate omission in the shared `build_workspace_bundle` row
mapping. The manifest assertion passed, then the combined direct/rebuilt
assertion failed: both row lists contained empty flags and null escalation.
Result: `1 failed in 0.47s`. Restored the original source bytes in a `finally`
block; a separate named-test rerun passed (`1 passed in 0.70s`). No deliberate
break remains in the working tree.

The unmodified pilot file timed out after 25 seconds at its HTTP test. A separate
socketpair send reproduced `PermissionError: [Errno 1] Operation not permitted`.
HTTP checks below use the temporary selector-wait cap described above, now at
`/tmp/pension-review-verification/sitecustomize.py`. It is outside the repository.

```console
python3 -m pytest tests/api/test_artifact_data.py::test_load_query_data_snapshot_same_document_revision_supersedes_for_trend -q -m "not slow"
# 4 passed in 0.30s

PYTHONPATH=/tmp/pension-review-verification python3 -m pytest tests/ops/test_one_pdf_pilot.py tests/export/test_workspace_bundle.py tests/web/test_build_workspace_bundle.py tests/web/test_workspace_contract.py tests/api/test_artifact_data.py tests/api/test_app_serving.py tests/ops/test_one_pdf_pilot_cli.py tests/query/test_metric_history.py tests/renderer/test_shell_import_smoke.py -q -m "not slow"
# 261 passed in 5.41s

BLACK_NUM_WORKERS=1 black --line-length 100 tests/api/test_artifact_data.py
# 1 file left unchanged.
PYTHONPATH=/tmp/pension-review-verification BLACK_NUM_WORKERS=1 black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
# 467 files would be left unchanged.
ruff check tests/api/test_artifact_data.py
# All checks passed!
python3 scripts/web/sync_renderer_shell.py --check
# Renderer-shell materialization is current.
git diff --check
# Passed.
```

The verified PR-body checkbox reconciliation was rejected by the GitHub connector:
`MCP tool call requires approval, but approval policy is never`. The local
checklist above records the verified state; the remote body was not changed.

After the formatting gate passed, staging the changed test and this transcript
failed creating `.git/index.lock` with `Read-only file system`. No new commit
was made. Publishing the blocker comment and adding `needs-human` were also
rejected with the same approval-policy message. A runner with Git write access
must commit these two files on the existing branch and reconcile the remote
checkboxes using the verified checklist above.

## Independent pilot review outcomes (2026-11-17)

Extended the existing pilot artifact-contract test from four authority cases to
16 combinations. Every declared tier and unknown authority is crossed with clear,
flags-only, escalation-only, and escalation-with-flags parser outcomes. Expected
context is pinned to the injected outcome, independently checked against the
parser artifact, manifest, and every nonempty direct bundle row. This catches
deriving escalation from authority or flag presence and dropping flags when
escalation is false. All existing artifact assertions remain intact.

Replayed the deliberate omission in the shared builder:

```python
ui_rows = [map_staging_row_to_ui_row(row) for row in rows]
```

The named test failed at `tests/ops/test_one_pdf_pilot.py:678` after the manifest
context equality passed. Both direct and rebuilt bundle row lists contained
`actionable_flags=[]` and `escalation_required=None` instead of the manifest's
two flags and `True`. Result: `1 failed in 0.39s`, exit code 1. The source was
restored byte-for-byte in a `finally` block; the named test then passed:
`1 passed in 0.67s`, exit code 0. No deliberate break remains.

The sandbox still rejects socketpair sends with `PermissionError: [Errno 1]
Operation not permitted`; the unmodified named-test command timed out after five
seconds. HTTP verification used the previously documented selector-wait cap,
at `/tmp/pension-review-outcome-runtime/sitecustomize.py`. This temporary file is
outside the repository and changes no application code or assertions.

```console
PYTHONPATH=/tmp/pension-review-outcome-runtime python3 -m pytest tests/ops/test_one_pdf_pilot.py -q -m "not slow"
# 35 passed in 0.65s

PYTHONPATH=/tmp/pension-review-outcome-runtime python3 -m pytest tests/ops/test_one_pdf_pilot.py tests/export/test_workspace_bundle.py tests/web/test_build_workspace_bundle.py tests/web/test_workspace_contract.py tests/api/test_artifact_data.py tests/api/test_app_serving.py tests/ops/test_one_pdf_pilot_cli.py tests/query/test_metric_history.py tests/renderer/test_shell_import_smoke.py -q -m "not slow"
# 273 passed in 5.21s

BLACK_NUM_WORKERS=1 black --line-length 100 tests/ops/test_one_pdf_pilot.py
# 1 file left unchanged.
PYTHONPATH=/tmp/pension-review-outcome-runtime BLACK_NUM_WORKERS=1 black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
# 467 files would be left unchanged.
ruff check tests/ops/test_one_pdf_pilot.py
# All checks passed!
python3 scripts/web/sync_renderer_shell.py --check
# Renderer-shell materialization is current.
git diff --check
# Passed.
```

- [x] First unchecked task: pilot validates authority and preserves review context in manifest/direct bundle across independent parser outcomes.
- [x] Named acceptance test passes, including API attribution, legacy unknowns, and simultaneous conditions.
- [x] Deliberate omission fails for both bundle paths while the manifest retains context; restoration passes.
- [x] Whole pilot test file passes; metadata tuple, renderer materialization, and existing API multi-run tests remain intact.
- [ ] Commit the changed Python test and evidence on the current branch. After the formatting gate passed, `git add tests/ops/test_one_pdf_pilot.py` failed creating `.git/index.lock`: `Read-only file system`.
- [ ] Publish evidence and reconcile remote checkboxes. PR-body update, verification comment, and additive `needs-human` label were each rejected: `MCP tool call requires approval, but approval policy is never`.

PR #1038 was confirmed open and ready (`draft=false`) at `4fd4aed`.

## Authority input paths and reconciliation (2026-11-17)

Reviewed the eight PR commits through `e29a188`. The implementation and previous
acceptance evidence are present, but the remote acceptance checkboxes remain
unchecked. Before editing, all 273 related tests passed. The attempted PR-body
reconciliation was rejected: `MCP tool call requires approval, but approval policy
is never`.

Extended the existing pilot artifact-contract test to cover direct construction,
argument resolution, and environment resolution. Each path exercises every
declared authority tier and unknown authority against four independent parser
outcomes. Declared values include surrounding whitespace; blank environment
metadata must remain unknown. The manifest and every nonempty direct bundle row
must retain normalized authority, escalation, and actionable flags. Existing
artifact assertions, metadata tuple, renderer checks, and API multi-run tests
remain intact.

Repeated the deliberate omission in the shared bundle builder. The manifest
assertion passed, then the named test failed at line 695: both direct and rebuilt
bundle contexts had empty flags and null escalation instead of the manifest's
two flags and `true`. Result: `1 failed in 0.36s`, exit 1. Restoring the original
source bytes in `finally` produced `1 passed in 0.47s`, exit 0. No deliberate
break remains.

An unmodified whole-file run timed out after 15 seconds in the HTTP test's
selector wait. Verification used the previously documented temporary selector
cap at `/tmp/pension-review-context-runtime/sitecustomize.py`; application code
and assertions were unchanged.

```console
PYTHONPATH=/tmp/pension-review-context-runtime python3 -m pytest tests/ops/test_one_pdf_pilot.py -q -m "not slow"
# 67 passed in 0.79s
PYTHONPATH=/tmp/pension-review-context-runtime python3 -m pytest tests/ops/test_one_pdf_pilot.py tests/export/test_workspace_bundle.py tests/web/test_build_workspace_bundle.py tests/web/test_workspace_contract.py tests/api/test_artifact_data.py tests/api/test_app_serving.py tests/ops/test_one_pdf_pilot_cli.py tests/query/test_metric_history.py tests/renderer/test_shell_import_smoke.py -q -m "not slow"
# 305 passed in 4.85s
PYTHONPATH=/tmp/pension-review-context-runtime BLACK_NUM_WORKERS=1 black --line-length 100 tests/ops/test_one_pdf_pilot.py
# 1 file left unchanged.
PYTHONPATH=/tmp/pension-review-context-runtime BLACK_NUM_WORKERS=1 black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
# 467 files would be left unchanged.
ruff check tests/ops/test_one_pdf_pilot.py
# All checks passed!
python3 scripts/web/sync_renderer_shell.py --check
# Renderer-shell materialization is current.
```

- [x] Pilot authority validation and manifest/direct bundle context verified through all input paths.
- [x] Bundle producer/runtime validation, manifest rebuild, API attribution, and synchronized renderer verified.
- [x] Named test and deliberate break/restoration verified.
- [x] Whole pilot file, optional metadata tuple, existing API multi-run tests, and renderer materialization verified.
- [ ] Commit on the existing branch: `git add` cannot create `.git/index.lock` (`Read-only file system`).
- [ ] Reconcile remote checkboxes and publish evidence: PR-body update, blocker comment, and additive `needs-human` label were rejected because approval is required and the approval policy is `never`.

## Skipped-rerun authority regression (2026-11-17)

Fixed the existing skipped-rerun test's vacuous bundle comparison: it previously
read both bundles after the second run had overwritten their shared path. It now
captures the first manifest and bundle before replay, proves that orchestration
skipped the document, and verifies four authority transitions: unknown unchanged,
unknown to declared, declared to unknown, and one declared tier to another.
Escalation and actionable flags remain present in each case; the complete bundle
comparison also verifies that the persisted metric rows and other fields survive.
No production change was needed. The previous deliberate-break/restoration
transcripts above remain applicable; no deliberate break was left in the tree.

```console
python3 -m pytest tests/ops/test_one_pdf_pilot.py::test_one_pdf_pilot_skipped_rerun_reuses_persisted_workspace_rows -q -m "not slow"
# 4 passed in 0.22s
PYTHONPATH=/tmp/pension-review-rerun-runtime python3 -m pytest tests/ops/test_one_pdf_pilot.py::test_escalation_and_declared_authority_survive_to_bundle_and_api -q -m "not slow"
# 1 passed in 0.45s
PYTHONPATH=/tmp/pension-review-rerun-runtime python3 -m pytest tests/ops/test_one_pdf_pilot.py tests/export/test_workspace_bundle.py tests/web/test_build_workspace_bundle.py tests/web/test_workspace_contract.py tests/api/test_artifact_data.py tests/api/test_app_serving.py tests/ops/test_one_pdf_pilot_cli.py tests/query/test_metric_history.py tests/renderer/test_shell_import_smoke.py -q -m "not slow"
# 308 passed in 4.91s, including all 70 pilot tests
PYTHONPATH=/tmp/pension-review-rerun-runtime BLACK_NUM_WORKERS=1 black --line-length 100 tests/ops/test_one_pdf_pilot.py
# 1 file left unchanged.
PYTHONPATH=/tmp/pension-review-rerun-runtime BLACK_NUM_WORKERS=1 black --check --line-length 100 --exclude '(\.workflows-lib|node_modules)' .
# 467 files would be left unchanged.
ruff check tests/ops/test_one_pdf_pilot.py
# All checks passed!
python3 scripts/web/sync_renderer_shell.py --check
# Renderer-shell materialization is current.
git diff --check
# Passed.
```

The unmodified named HTTP test timed out after 20 seconds. A socketpair send
independently raised `PermissionError: [Errno 1] Operation not permitted`.
The HTTP commands above use the previously documented temporary selector-wait
cap, located at `/tmp/pension-review-rerun-runtime/sitecustomize.py`; application
code and assertions were unchanged by that workaround.

- [x] Pilot authority validation and manifest/direct bundle propagation verified.
- [x] Producer/runtime contracts, rebuild, API attribution, and renderer checks pass.
- [x] Named test and whole pilot file pass; metadata tuple includes authority and existing API multi-run tests are retained.
- [ ] Commit regression test and this evidence on the existing branch: `git add tests/ops/test_one_pdf_pilot.py` failed creating `.git/index.lock` with `Read-only file system`.
- [ ] Reconcile PR checkboxes, publish the verification/blocker comment, and add `needs-human`: all three connector mutations were rejected with `MCP tool call requires approval, but approval policy is never`.

PR #1038 was confirmed open and ready (`draft=false`) at `2395b55`. No branch,
PR, workflow, or protected configuration was created or changed by this run.

## CLI authority propagation and checklist reconciliation (2026-11-17)

Reviewed the recent commits through `4d217e1` before continuing. The four
implementation tasks are present. Added eight CLI regression cases: every
`SOURCE_AUTHORITY_TIERS` value reaches both the manifest and each direct bundle
row through flags and environment fallback; flags override stale environment
authority; invalid authority is rejected before starting the pilot or creating
output, including when a valid environment value exists.

- [x] Pilot authority validation and manifest/direct bundle persistence verified.
- [x] Bundle producer/runtime contracts and manifest rebuild verified.
- [x] API per-document attribution and renderer synchronization verified.
- [x] Named acceptance test: 1 passed; whole pilot file: 70 passed.
- [x] Existing optional metadata tuple, API multi-run tests, and renderer checks retained.
- [x] Existing deliberate-break/restoration evidence reviewed above.

Verification: the related pilot, CLI, bundle, runtime, API, query, and renderer
suite passed **316 tests** with `-m "not slow"`. The named acceptance test and
whole pilot file also passed separately. Renderer materialization and Ruff
passed. Black formatted the changed Python file at line length 100; the required
repository check reported **467 files would be left unchanged**. HTTP/socket and
Black verification use the documented temporary selector-wait cap under
`/tmp/pension-authority-test-runtime`; no workaround is committed.

- [ ] Commit the changes: `git add` cannot create `.git/index.lock` because the
  repository's `.git` directory is read-only.
- [ ] Reconcile the remote task checkboxes: automatic approval review rejected
  the PR-body update, blocker comment, and additive `needs-human` label with
  `MCP tool call requires approval, but approval policy is never`.

PR #1038 remains open and ready (`draft=false`). No branch or PR was created.

## Acceptance reconciliation and missing-row checks (2026-11-17)

Reviewed the six recent commits through `af5e998` and the implementation. All four
implementation tasks are already implemented and verified. The existing deliberate
break/restoration evidence above remains applicable. The remote checkboxes are
stale; the attempted reconciliation was rejected by automatic approval review.

Strengthened the named acceptance test so empty bundle row lists cannot satisfy
its context assertions. Direct and rebuilt bundles must match in full, and both
the API snapshot and HTTP response must retain every document/metric identity
from the three contributing bundles. Existing metadata tuple, renderer, and API
multi-run assertions remain intact.

Verification used `PYTHONPATH=/tmp/pension-context-verification` with the previously
documented temporary selector-wait cap. An unmodified pilot test run hung in the
HTTP client's selector wait; its 15-second traceback confirmed that limitation.
Black also used `BLACK_NUM_WORKERS=1`. No workaround is committed.

- [x] Pilot authority validation and manifest/direct bundle persistence verified.
- [x] Bundle producer/runtime contracts and manifest rebuild verified.
- [x] API attribution, renderer materialization, and existing multi-run tests verified.
- [x] Related baseline suite: 308 passed in 6.37s.
- [x] Strengthened named acceptance test: 1 passed in 0.65s.
- [x] Whole pilot file after the change: 70 passed in 1.30s.
- [x] Repository-wide required Black check: 467 files unchanged; targeted Ruff,
      renderer synchronization, and `git diff --check` passed.
- [ ] Commit on the checked-out branch: `git add tests/ops/test_one_pdf_pilot.py`
      failed creating `.git/index.lock` with `Read-only file system`.
- [ ] Update remote task/acceptance checkboxes, publish verification/blocker
      comment, and add `needs-human`: all three mutations were rejected with
      `MCP tool call requires approval, but approval policy is never`.

## Skipped-rerun API attribution verification (2026-11-17)

At `0ecbddf`, extended the existing skipped-rerun regression to inspect API
snapshots before and after all four authority transitions. Both snapshots must
retain every bundle document/metric identity. Every row must carry the current
authority alongside escalation and actionable flags, while all other metric
fields remain equal across the replay. Existing bundle assertions remain intact.

- [x] Pilot declaration validation and manifest/direct bundle propagation verified.
- [x] Producer/runtime contracts, manifest rebuild, and API attribution verified.
- [x] Named acceptance test: 1 passed; whole pilot file: 70 passed.
- [x] Related bundle, rebuild, runtime, API, CLI, query, and renderer tests: 246 passed.
- [x] Optional metadata tuple includes authority; existing API multi-run and renderer checks pass.
- [x] Required repository Black check: 467 files unchanged; targeted Ruff and diff checks pass.
- [x] Existing deliberate-break/restoration evidence above remains applicable.

All pytest commands used `-m "not slow"`. The unmodified named test timed out
after 15 seconds in asyncio's selector; a socketpair send independently raised
`PermissionError: [Errno 1] Operation not permitted`. HTTP verification and the
repository Black check used the previously documented temporary selector-wait
cap at `/tmp/pension-replay-api-runtime/sitecustomize.py`; Black also used
`BLACK_NUM_WORKERS=1`. No runtime workaround is committed.

- [ ] Commit on the checked-out branch: `git add tests/ops/test_one_pdf_pilot.py`
      failed creating `.git/index.lock` with `Read-only file system`.
- [ ] Reconcile remote checkboxes, publish the blocker comment, and add
      `needs-human`: automatic approval review rejected all three operations with
      `MCP tool call requires approval, but approval policy is never`.

PR #1038 was confirmed open and ready (`draft=false`) at `0ecbddf`.

## Review outcome changes on skipped reruns (2026-11-17)

At `6a459eb`, extended the existing skipped-rerun test across four parser review
transitions: escalation retained, added, cleared, and flags replaced without
escalation. Each runs across the four existing authority transitions. The
manifest, direct bundle, and API snapshot must carry the latest review outcome
while preserving every metric identity and all other metric fields.

- [x] All 16 skipped-rerun cases pass without a runtime workaround.
- [x] Named escalation/authority/bundle/API/legacy acceptance test passes.
- [x] Whole pilot file passes (82 cases), as part of 328 related tests.
- [x] Existing metadata tuple, multi-run API, and renderer materialization checks pass.
- [x] Black formats the changed Python file; required repository check reports
      467 files unchanged. Targeted Ruff and `git diff --check` pass.

All pytest commands used `-m "not slow"`. The unmodified baseline reached the
named test's HTTP request and stalled in asyncio's selector; an independent
socketpair send raised `PermissionError: [Errno 1] Operation not permitted`.
HTTP verification and the repository Black check used the documented temporary
selector-wait cap at `/tmp/pension-review-refresh-runtime/sitecustomize.py`;
Black also used `BLACK_NUM_WORKERS=1`. No runtime workaround is committed.

- [ ] Commit the Python test and evidence on the current branch: `git add`
      failed creating `.git/index.lock` with `Read-only file system`.
- [ ] Reconcile verified PR task checkboxes: automatic approval review rejected
      the update with `MCP tool call requires approval, but approval policy is never`.
      The verification/blocker comment and additive `needs-human` label were
      rejected with the same message; no remote metadata changed.

PR #1038 was confirmed open and ready (`draft=false`) at `6a459eb`.
