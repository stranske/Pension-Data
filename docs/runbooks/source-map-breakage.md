# Source Map Breakage

Last reviewed: 2026-03-02
Incident class: `source_map_breakage`

## Symptoms

- CI or gate fails on source-map lint or validation.
- Discovery jobs fail before crawl begins.
- New source entries are rejected with duplicate/conflict findings.

## Diagnostic Commands

```bash
rg -n "source_map_breakage|source-map-breakage" docs/ops docs/runbooks
```

```bash
pytest -q tests/docs/test_runbook_presence.py
```

## Remediation Steps

1. Run lint and capture failing finding codes from output.
2. Fix malformed URL/domain/authority-tier fields in source-map config.
3. Re-run lint until output is `OK`.
4. Re-run source validation tests to ensure edge cases remain covered.
5. Push fix and verify Gate is green.

## Expected Signals

- Source-map lint exits with status `0`.
- Gate reports `Python CI / lint-ruff` and `Gate / gate` passing.
- No new duplicate/conflicting seed URL findings appear in PR checks.
