# LangChain Evaluation Harness

This harness provides deterministic regression checks for NL features with two modes:

- `mock`: uses committed recorded outputs for fast CI and local checks.
- `live`: runs a user-provided command per case for manual provider validation.

## Dataset Contract

Default dataset path: `tests/langchain/prompt_dataset.json`

Required top-level keys:

- `version` (int)
- `cases` (list)

Optional top-level key:

- `thresholds` with each value in `[0, 1]`:
  - `min_schema_validity_rate`
  - `min_citation_coverage_rate`
  - `min_no_hallucination_rate`
  - `min_safety_pass_rate`

Case fields:

- `id`
- `domain`
- `feature` (`nl_sql`, `findings_explain`, or `findings_compare`)
- `question`
- `recorded_output` (required for `mock` mode)
- `expected_sql_contains` (optional)
- `expected_citations` (optional)
- `allowed_relations` (optional)

## Command

```bash
python scripts/langchain/eval_runner.py \
  --dataset tests/langchain/prompt_dataset.json \
  --mode mock \
  --output artifacts/langchain/eval_report.json
```

The command exits non-zero on regression failure.

## Live Mode

Live mode is optional and manual. Provide a command that:

- reads one case JSON object from stdin
- prints one JSON object to stdout

Example:

```bash
python scripts/langchain/eval_runner.py \
  --mode live \
  --live-command "python scripts/langchain/my_live_adapter.py" \
  --output artifacts/langchain/eval_report.live.json
```

## Current Metrics

- Schema validity rate
- Citation coverage rate
- No-hallucination rate
- Safety pass rate

Safety regressions force a failing status.
