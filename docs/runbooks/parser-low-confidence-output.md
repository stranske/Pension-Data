# Parser Low Confidence Output

Last reviewed: 2026-03-03
Incident class: `parser_low_confidence_output`

## Symptoms

- Parser output validates structurally but confidence routing places rows in high-priority review.
- Operators see manual-review queue growth without hard promotion blocks.
- Metrics with weak evidence quality recur across similar documents or layouts.

## Diagnostic Commands

```bash
gh run view "$RUN_ID" --log > /tmp/parser-low-confidence.log
rg -n "parser_low_confidence_output|high_priority_review|confidence" /tmp/parser-low-confidence.log
```
Expected signal: high-priority routed rows include metric names, confidence values, and evidence refs.

```bash
pytest -q tests/quality/test_confidence_routing.py tests/quality/test_parser_output_validation.py
```
Expected signal: confidence thresholds and parser-review routing behavior are deterministic and reproducible.

## Remediation Steps

1. Enumerate the top recurring low-confidence metrics by plan and period from review queue exports.
2. Validate evidence quality for each metric and confirm the parser captures the strongest available anchors.
3. Improve parser extraction logic for recurring weak patterns while preserving deterministic output ordering.
4. Add targeted parser fixtures that reproduce the low-confidence case with clear acceptance assertions.
5. Re-run confidence and parser validation tests to verify queue routing drops for corrected cases.
6. Monitor subsequent runs and only close the incident after low-confidence volume returns to expected levels.

## Expected Signals

- Fewer parser rows route to high-priority review for unchanged document cohorts.
- Confidence scores improve in affected metrics without introducing schema/provenance regressions.
- Operator queue throughput returns to normal.
