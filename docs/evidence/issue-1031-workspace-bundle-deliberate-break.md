# Issue #1031 / workspace bundle pilot validation deliberate-break transcript

## Named test

`tests/web/test_build_workspace_bundle.py::test_builder_rejects_failed_or_foreign_pilot_artifacts`

The `absolute-outside-run` case points the manifest at another run's staging file whose
`plan_id`, `plan_period`, and `source_document_id` still match the selected manifest, so
a reverted `_artifact_path` early return defeats containment specifically rather than
identity checks alone.

## 1. Gate with containment restored on HEAD (PASS)

```text
$ PYTHONPATH=src python3 -m pytest \
    tests/web/test_build_workspace_bundle.py::test_builder_rejects_failed_or_foreign_pilot_artifacts -q

.......                                                                  [100%]
7 passed
```

Automated replay against `origin/main`'s `scripts/web/build_workspace_bundle.py`:

```text
$ python3 scripts/check_deliberate_break.py --pr-body-file <marker-with break-file>
{"verdict": "PASS", "reason": "head-passed-base-failed", ...}
```

All seven parametrized cases fail on the base file (seven `DID NOT RAISE ValueError`
failures), including `absolute-outside-run`.

## 2. Deliberate break on HEAD (expected failure)

Temporarily restore the pre-containment branch in `_artifact_path`:

```python
    candidate = Path(raw_path.strip())
    if candidate.is_absolute() or candidate.exists():
        return candidate
```

```text
$ PYTHONPATH=src python3 -m pytest \
    tests/web/test_build_workspace_bundle.py::test_builder_rejects_failed_or_foreign_pilot_artifacts[absolute-outside-run] -q

F                                                                        [100%]
FAILED ...[absolute-outside-run] - Failed: DID NOT RAISE ValueError
```

## 3. Revert and confirm full file (PASS)

After restoring the containment check and CLI validation guard:

```text
$ PYTHONPATH=src python3 -m pytest tests/web/test_build_workspace_bundle.py -q

..........                                                               [100%]
10 passed
```

The parametrized test also keeps a pre-existing `--out` file unchanged when the CLI
rejects a mutated manifest (`assert out.read_bytes() == before`).