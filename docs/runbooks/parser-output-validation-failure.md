# Parser Output Validation Failure

Last reviewed: 2026-03-03
Incident class: `parser_output_validation_failure`

## Symptoms

- Parser output rows fail schema, numeric sanity, or provenance validation checks.
- Promotion to curated facts is blocked for affected plan-period outputs.
- Review queue receives parser-validation failure rows with high priority.

## Diagnostic Commands

```bash
gh run view "$RUN_ID" --log > /tmp/parser-output-validation-failure.log
rg -n "parser_output_validation_failure|schema_invalid|numeric_out_of_range|provenance_" /tmp/parser-output-validation-failure.log
```
Expected signal: validation finding codes and impacted metric names are listed with runbook links.

```bash
pytest -q tests/quality/test_parser_output_validation.py tests/review_queue/test_extraction_queue.py
```
Expected signal: validation and review-queue contract tests reproduce the issue before a fix and pass after remediation.

## Remediation Steps

1. Identify the failing plan-period rows and record each finding code and metric.
2. Confirm the parser output contract fields are present and string identifiers are non-empty.
3. Correct numeric normalization or metric-range handling so values fall in accepted ranges.
4. Update provenance emitters to produce canonical `p.`, `text:`, or `table:` evidence refs.
5. Re-run parser validation tests and confirm promotion is no longer blocked for the fixed fixture.
6. Submit the parser and test updates together so regression coverage protects the fix.

## Expected Signals

- Blocking findings clear for affected parser outputs.
- Promotion resumes for corrected outputs without manual data edits.
- Review queue volume for validation failures returns to baseline.
