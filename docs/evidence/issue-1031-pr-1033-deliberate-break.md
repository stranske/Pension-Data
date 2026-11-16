# Issue #1031 / PR #1033 deliberate-break transcript

Fresh replay on `cursor/issue-1031` (2026-11-16) for acceptance criteria A1–A4.

## Provenance

- Branch: `cursor/issue-1031` at keepalive round 4/12 (acceptance replay after hollow_break on round 4 dispatch)
- Focused test: `tests/web/test_build_workspace_bundle.py::test_builder_rejects_failed_or_foreign_pilot_artifacts`
- Break target: `scripts/web/build_workspace_bundle.py` (`_artifact_path`)
- Environment: Python 3.12.2 via `/opt/anaconda3/bin/python3`; `PYTHONPATH=src`

## Baseline (head)

```console
python3 -m pytest tests/web/test_build_workspace_bundle.py::test_builder_rejects_failed_or_foreign_pilot_artifacts -q
python3 -m pytest tests/web/test_build_workspace_bundle.py -q
```

```text
7 passed in 0.60s  (named parametrized test)
10 passed in 0.61s  (full file)
```

```console
PYTHONPATH=src python3 scripts/check_deliberate_break.py --head HEAD --pr-body-file <pr-body>
```

```text
verdict: PASS (head-passed-base-failed)
```

## Deliberate break: expected failure

Temporarily restored the pre-fix early return in `_artifact_path`:

```python
    if candidate.is_absolute() or candidate.exists():
        return candidate
```

```console
PYTHONPATH=src python3 -m pytest tests/web/test_build_workspace_bundle.py::test_builder_rejects_failed_or_foreign_pilot_artifacts -q -o addopts=
```

```text
F......                                                                  [100%]
FAILED ...[absolute-outside-run] - Failed: DID NOT RAISE ValueError
1 failed, 6 passed in 0.59s
```

Only `absolute-outside-run` regressed: other cases still fail on containment or identity checks even with the early return.

## Restore

Removed the early return (current HEAD). Named test passes again (7 passed).