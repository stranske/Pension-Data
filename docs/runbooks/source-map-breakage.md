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

1. Confirm active failure and capture finding codes from CI logs (`SCHEMA_*`, `URL_*`, `DUPLICATE_*`).
2. Identify the exact source-map entry changed in the failing revision.
3. Correct malformed fields (URL, domain, authority tier, plan labels) in the source-map config for that entry.
4. Re-run source-map lint locally and stop only when it exits `0` with no findings.
5. Re-run runbook/doc quality checks to ensure references are still valid.
6. Open a PR with the fix and confirm both lint and gate checks pass before closing the incident.

## Expected Signals

- Source-map lint exits with status `0`.
- Gate reports `Python CI / lint-ruff` and `Gate / gate` passing.
- No new duplicate/conflicting seed URL findings appear in PR checks.
