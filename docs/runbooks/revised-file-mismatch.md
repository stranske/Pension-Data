# Revised File Mismatch

Last reviewed: 2026-03-02
Incident class: `revised_file_mismatch`

## Symptoms

- Newly discovered annual reports do not reconcile to expected plan period.
- Supersession logic cannot determine which version is authoritative.
- Coverage outputs regress after report revisions.

## Diagnostic Commands

```bash
gh run view "$RUN_ID" --log > /tmp/revised-file-mismatch.log
rg -n "revised_file_mismatch|supersession|plan[ _-]?period|mismatch" /tmp/revised-file-mismatch.log
```
Expected signal: log lines include the exact plan ID, period, and mismatch reason to investigate.

```bash
rg -n "revised|supersession|authoritative|period" docs/ops docs/runbooks docs/contracts
```
Expected signal: canonical matching/supersession docs are identified for side-by-side metadata comparison.

```bash
pytest -q tests/test_main.py tests/test_dependency_version_alignment.py
```
Expected signal: targeted registry/source tests reproduce the mismatch pre-fix and pass after correction.

## Remediation Steps

1. Pull the failing artifact/log bundle and record the exact plan ID, filing period, and mismatch reason emitted.
2. Compare revised document metadata against canonical source-map metadata for the same plan and period.
3. If identity keys differ, fix plan/period mapping in the source-map entry; if URL lineage differs, fix supersession metadata.
4. Re-run local checks that exercise revised-file reconciliation and confirm mismatch counts return to expected baseline.
5. Validate that revised rows now map to a single authoritative document for each affected period.
6. Submit the metadata fix and verify CI classifies revised rows without mismatch regressions.

## Expected Signals

- Mismatch findings drop to expected baseline after metadata correction.
- CI tests for registry and sources complete without failures.
- Coverage/readiness outputs show consistent period assignment.
