# One-PDF Pilot Runbook

## Purpose

Run a deterministic single-document pilot that emits parser, orchestration, persistence, and coverage summary artifacts for one pension PDF.

## Prerequisites

1. Python environment is available (`uv` preferred).
2. Repo dependencies are installed (`uv sync --dev`).
3. You have a local pension PDF path that is readable.
4. If the PDF is image-only, OCR fallback is not currently wired in this harness; extraction can fail with missing required metrics.

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
  - `--default-money-unit-scale` (`usd|thousand_usd|million_usd|billion_usd`)
  - `--run-id`

## Run Command

```bash
uv run python scripts/run_one_pdf_pilot.py \
  --pdf-path "/absolute/path/to/pension.pdf" \
  --plan-id "CA-PERS" \
  --plan-period "FY2024" \
  --effective-date "2024-06-30" \
  --ingestion-date "2026-03-03" \
  --output-root "outputs"
```

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

## Troubleshooting

1. Missing required funded metrics:
   - Symptom: CLI exits with `Unable to parse required funded metrics`.
   - Action: validate PDF text extractability and confirm required labels exist.
2. OCR not configured for image-only PDFs:
   - Symptom: parser result flags OCR fallback gaps and no raw extraction payload.
   - Action: run OCR externally first, then retry with a text-searchable PDF.
3. Empty persistence outputs:
   - Symptom: `staging_core_metrics.json` is empty.
   - Action: inspect `parser_result.json` and `document_orchestration/<run-id>/ledger.json` for parse/validation failures.
