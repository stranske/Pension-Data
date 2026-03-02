# Revised File Mismatch

Last reviewed: 2026-03-02
Incident class: `revised_file_mismatch`

## Symptoms

- Newly discovered annual reports do not reconcile to expected plan period.
- Supersession logic cannot determine which version is authoritative.
- Coverage outputs regress after report revisions.

## Diagnostic Commands

```bash
pytest -q tests/test_main.py tests/test_dependency_version_alignment.py
```

```bash
rg -n "revised_file_mismatch|revised-file-mismatch|mismatch" docs/ops docs/runbooks
```

## Remediation Steps

1. Confirm plan identity and period labels in source-map metadata.
2. Verify revised document URL belongs to the expected plan.
3. Update mismatch reason or period mapping fields as required.
4. Re-run registry/source tests and validate no conflicting identity keys.
5. Push fix and verify discovery outputs classify revised rows correctly.

## Expected Signals

- Mismatch findings drop to expected baseline after metadata correction.
- CI tests for registry and sources complete without failures.
- Coverage/readiness outputs show consistent period assignment.
