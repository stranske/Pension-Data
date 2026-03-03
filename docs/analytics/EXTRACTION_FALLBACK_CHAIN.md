# Extraction Fallback Chain and Golden Regression Harness

Issue: #25

## Fallback Order

Configured in `src/pension_data/extract/orchestration/fallback.py`:

- funded: `table_primary -> text_fallback -> full_fallback`
- actuarial: `table_primary -> text_fallback -> full_fallback`
- investment: `table_primary -> text_fallback -> full_fallback`

## Retry + Escalation

`run_fallback_chain(...)` executes parser stages in order and records structured `ParserAttempt` rows.

When all stages fail required completeness checks, it emits:

- `EscalationEvent`
  - `reason = parser_fallback_exhaustion`
  - exhausted stage count
  - attempt-by-attempt failure details

## Golden Corpus Harness

The funded fallback parser used for regression snapshots lives at:

- `tools/golden_extract/fallback_extract_parser.py`

Golden corpus and baseline:

- `tests/golden/extraction_fallback_corpus.json`
- `tests/golden/extraction_fallback_baseline.json`

## Regression Diff and CI Gate

Workflow:

- `.github/workflows/extraction-golden-regression.yml`

Pipeline steps:

1. Replay corpus into current snapshot
2. Diff baseline vs current (field-level + confidence/evidence drift)
3. Gate on unexpected drift (`max_unexpected = 0`)
4. Upload snapshot + diff + gate reports as artifacts
