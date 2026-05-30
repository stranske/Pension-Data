# Query Run Record Contract

Last reviewed: 2026-05-30
Related issue: #481

## Purpose

SQL and NL-to-SQL requests emit one replayable JSON run record per request. The record is additive to the existing SQL audit log and NL JSONL operation log; those legacy shapes remain stable.

## Record Fields

- `run_id`: existing `query_id` or `request_id`.
- `surface`: `sql` or `nl`.
- `status`: `ok` or `error`.
- `who`: caller key ID, granted scopes, required scope, and optional correlation ID.
- `inputs`: validated request inputs.
- `generated_sql`: provider/chain-generated SQL for NL requests, otherwise `null`.
- `executed_sql`: SQL sent to the database.
- `columns` and `row_count`: returned table shape.
- `rows_artifact`: pointer to the persisted row payload.
- `provenance`: per-row NL provenance summary when available.
- `warnings` and `error`: deterministic warning/error blocks.
- `duration_ms`: request duration.
- `cost`: token/cost block for NL provider responses, or `null` for plain SQL.
- `artifacts`: named artifacts created by the run.

## Artifact Layout

Artifacts are written under the local `artifacts/` root:

- `artifacts/query/sql_runs/runs/<query_id>.json`
- `artifacts/query/sql_runs/rows/<query_id>.json`
- `artifacts/langchain/nl_runs/runs/<request_id>.json`
- `artifacts/langchain/nl_runs/rows/<request_id>.json`

Run IDs are path-normalized by replacing non-alphanumeric separators with `-`. JSON is written with sorted keys.

## Replay

`replay_run_record` reconstructs an NL response from the run record and rows artifact, including rows and per-row provenance. This complements `replay_logged_request`, which can only replay from the lossy JSONL log and logged SQL text.

## LLM Boundary

The run record does not introduce any external calls. NL token and cost fields are copied only from metadata already returned by the chain/provider response mapping. Plain SQL records always set `cost` to `null`.
