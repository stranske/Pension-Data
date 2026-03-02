# Parser Fallback Exhaustion

Last reviewed: 2026-03-02
Incident class: `parser_fallback_exhaustion`

## Symptoms

- Extraction pipeline reports all parser strategies failed.
- Required metrics are missing for one or more plan-year rows.
- Review queue receives high volume of low-confidence extraction artifacts.

## Diagnostic Commands

```bash
pytest -q tests/test_main.py tests/test_dependency_version_alignment.py
```

```bash
pytest -q tests/docs/test_runbook_presence.py
```

## Remediation Steps

1. Identify failing parser stage from pipeline logs.
2. Validate source-document type hints and authority-tier metadata.
3. Add or adjust parser rule in fallback chain for the failing format.
4. Add fixture coverage that reproduces the failure mode.
5. Re-run targeted extraction tests and verify confidence routing behavior.

## Expected Signals

- At least one parser path succeeds for previously failing fixtures.
- Fallback exhaustion alerts return to baseline.
- Required extraction checks pass in CI.
