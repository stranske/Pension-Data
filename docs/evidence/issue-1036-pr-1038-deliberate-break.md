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
