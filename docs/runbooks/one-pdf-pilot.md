# One-PDF Pilot Runbook

## Purpose

Run a deterministic single-document pilot that emits parser, orchestration, persistence, and coverage summary artifacts for one pension PDF.

## Prerequisites

1. Python 3.11+ is available (`uv` preferred for local execution).
2. Repo dependencies are installed from the repo root:
   - `uv sync --dev`
3. You have a local pension PDF path that is readable by the current user.
4. If the PDF is image-only, OCR fallback is not currently wired in this harness; extraction can fail with missing required metrics.

## Quick Start

Run from the repository root:

```bash
uv run python scripts/run_one_pdf_pilot.py \
  --pdf-path "/absolute/path/to/pension.pdf" \
  --plan-id "CA-PERS" \
  --plan-period "FY2024" \
  --effective-date "2024-06-30" \
  --ingestion-date "2026-03-03" \
  --output-root "outputs"
```

## Canonical Input Contract

- `--pdf-path`: local file path to one pension PDF.
- `--plan-id`: canonical plan identifier.
- `--plan-period`: canonical period label (for example `FY2024`).
- `--effective-date`: reporting effective date (`YYYY-MM-DD`).
- `--ingestion-date`: ingestion date (`YYYY-MM-DD`).
- Optional metadata:
  - `--source-url`
  - `--source-document-id`
  - `--fetched-at`
  - `--mime-type` (defaults to `application/pdf`)
  - `--default-money-unit-scale` (`usd|thousand_usd|million_usd|billion_usd`)
  - `--run-id`

Environment variable fallback is supported for all required inputs:

- `ONE_PDF_PILOT_PDF_PATH`
- `ONE_PDF_PILOT_PLAN_ID`
- `ONE_PDF_PILOT_PLAN_PERIOD`
- `ONE_PDF_PILOT_EFFECTIVE_DATE`
- `ONE_PDF_PILOT_INGESTION_DATE`

## Expected Artifacts

Artifacts are written under:

`<output-root>/one_pdf_pilot/<run-id>/`

Required files:

- `run_manifest.json`
- `parser_result.json`
- `coverage/component_coverage_summary.json`
- `extraction_persistence/persistence_contract.json`
- `extraction_persistence/staging_core_metrics.json`
- `extraction_persistence/staging_manager_fund_vehicle_relationships.json`
- `extraction_persistence/extraction_warnings.json`
- `extraction_persistence/component_datasets_manifest.json`
- `document_orchestration/<run-id>/ledger.json`
- `document_orchestration/<run-id>/published_rows.json`
- `document_orchestration/<run-id>/review_queue_rows.json`
- `document_orchestration/<run-id>/state.json`

Coverage summary (`coverage/component_coverage_summary.json`) includes:

- `missing_required_metrics`
- `has_required_funded_metrics`
- `escalation_required`
- `published_row_count`
- `review_queue_row_count`
- `staging_core_metric_count`
- `relationship_row_count`
- `warning_row_count`

## Troubleshooting

1. Missing required fields:
   - Symptom: CLI exits with `Missing required one-pdf pilot input fields`.
   - Action: pass missing flags or set corresponding `ONE_PDF_PILOT_*` environment variables.
2. PDF path missing/unreadable:
   - Symptom: CLI exits with `FileNotFoundError: PDF not found`.
   - Action: verify `--pdf-path` points to a local readable file and use an absolute path when possible.
3. Missing required funded metrics:
   - Symptom: CLI exits with `Unable to parse required funded metrics`.
   - Action: validate PDF text extractability and confirm required labels exist in the source document.
4. OCR not configured for image-only PDFs:
   - Symptom: parser cannot produce funded metrics and exits with the same required-metrics failure.
   - Action: run OCR outside this harness, then retry with a text-searchable PDF.
5. Invalid money unit scale:
   - Symptom: CLI exits with `default_money_unit_scale must be one of ...`.
   - Action: use one of `usd|thousand_usd|million_usd|billion_usd`.
6. Empty persistence outputs:
   - Symptom: `staging_core_metrics.json` is empty or `published_row_count` is `0`.
   - Action: inspect `parser_result.json`, `coverage/component_coverage_summary.json`, and `document_orchestration/<run-id>/ledger.json` for parse/validation failures.
