# Document Orchestration Job Contract

Last reviewed: 2026-03-03
Related issue: #129

## Input Contract

`SourceDocumentJobItem` fields:

- `plan_id`
- `plan_period`
- `source_url`
- `fetched_at`
- `mime_type`
- `content_bytes`
- `source_document_id`
- `effective_date`
- `ingestion_date`
- `default_money_unit_scale`

## Stage Flow

1. `discovery`: validate required source-document fields.
2. `ingestion`: run immutable artifact ingestion + supersession lineage updates.
3. `parse_extract`: call parser and funded/actuarial extractor.
4. `validation`: route low-confidence rows to review queue and block missing required metrics.
5. `publish`: persist non-duplicate fact rows and write run artifacts.

## Idempotency Guarantees

- Re-running the same artifact does not re-promote duplicate facts.
- `processed_artifact_ids` prevents duplicate parse/extract work.
- `published_fact_ids` prevents duplicate final-fact promotion.

## Revised Document Semantics

- Revised content creates a new active artifact with `supersedes_artifact_id`.
- Revised artifacts are reprocessed and produce new publish rows.
- Prior artifact lineage is preserved through immutable artifact records.

## Retry + Failure Recovery

- Parser/extract stage retries are configurable via `max_retries_per_stage`.
- Failures are recorded as structured `OrchestrationFailure` rows with stage + message.
- Runs continue processing unaffected documents in the same batch.
