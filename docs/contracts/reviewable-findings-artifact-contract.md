# Reviewable Findings Artifact Contract

## Source Of Truth

- Schema artifact: `docs/data/reviewable-findings/findings.schema.json`
- First published payload path: `docs/data/reviewable-findings/extraction-quality-dashboard.json`
- Python helper: `src/pension_data/langchain/review_artifact.py`
- Contract tests: `tests/langchain/test_review_artifact_contract.py`

## First Slice

The first reviewable UI/LangChain slice is `extraction_quality_dashboard`.

This slice is intentionally narrower than the full web workspace. It reports extraction quality
findings that a reviewer can inspect in a static browser UI and ask asynchronous LangChain
explain/compare questions about.

## Artifact Envelope

Required top-level fields:

- `artifact_type`: always `pension_data.reviewable_findings`
- `schema_version`: integer contract version, starting at `1`
- `artifact_id`: stable id for one generated payload
- `generated_at`: UTC timestamp
- `source_artifact_ids`: non-empty identifiers for upstream extraction/readiness artifacts
- `slice`: metadata for the dashboard slice
- `findings`: finding rows
- `langchain_actions`: supported asynchronous interaction contracts

## Finding Rows

Each finding row must include:

- `finding_id`
- `entity`
- `period`
- `metric_family`
- `metric`
- `value`
- `confidence`
- `provenance_refs`
- `citations`

Optional `severity` values are `info`, `warning`, and `blocker`.

The required filter fields for the static UI are `entity`, `period`, `metric_family`, and
`confidence`. Citations and provenance references are mandatory so the UI can display source
tooltips and LangChain outputs can stay citation-bound.

## LangChain Actions

The artifact advertises two asynchronous actions:

- `explain`: accepts one or more `finding_ids` plus a question and writes a structured summary.
- `compare`: accepts two or more `finding_ids` plus a question and writes a structured comparison.

Outputs must include `request_id`, `generated_at`, `summary`, `citations`, and `artifact_path`.

## Generation Plan

The first generator should read extraction persistence outputs and source-readiness artifacts, then
write `docs/data/reviewable-findings/extraction-quality-dashboard.json` as a CI/release artifact.
It should fail validation with `validate_reviewable_findings_artifact(...)` before publishing.
