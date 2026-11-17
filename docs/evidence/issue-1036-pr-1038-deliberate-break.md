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
