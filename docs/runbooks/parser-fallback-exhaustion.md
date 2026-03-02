# Parser Fallback Exhaustion

Last reviewed: 2026-03-02
Incident class: `parser_fallback_exhaustion`

## Symptoms

- Extraction pipeline reports all parser strategies failed.
- Required metrics are missing for one or more plan-year rows.
- Review queue receives high volume of low-confidence extraction artifacts.

## Diagnostic Commands

```bash
gh run view "$RUN_ID" --log > /tmp/parser-fallback-exhaustion.log
rg -n "parser_fallback_exhaustion|fallback|stage|required fields|extract" /tmp/parser-fallback-exhaustion.log
```
Expected signal: a full parser-stage trace shows where fallback paths were exhausted.

```bash
rg -n "fallback|parser|extract|required" scripts src tests
```
Expected signal: likely parser rules/fixtures tied to the failing stage are identified for focused edits.

```bash
pytest -q tests/test_main.py tests/test_dependency_version_alignment.py
```
Expected signal: targeted extraction/regression tests fail before the parser fix and pass afterward.

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
