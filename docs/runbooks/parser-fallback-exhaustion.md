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

1. Extract the failing document identifier and the full parser-stage trace from pipeline logs.
2. Confirm the document type hints and authority-tier metadata are correct for that document.
3. Determine whether the failure is metadata misrouting or a missing parser rule in the fallback chain.
4. Apply the minimal fix: correct metadata routing or add/adjust the parser rule for the failing format.
5. Add or update a regression fixture that reproduces the exact exhaustion path.
6. Re-run targeted extraction tests and close the incident only when at least one parser path succeeds and required fields are present.

## Expected Signals

- At least one parser path succeeds for previously failing fixtures.
- Fallback exhaustion alerts return to baseline.
- Required extraction checks pass in CI.
