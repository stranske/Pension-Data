# LangSmith Fleet Records (NL-to-SQL)

Pension-Data emits Workflows-compatible `langsmith-fleet/v1` NDJSON records
for the NL-to-SQL surface so the fleet dashboard can flag missing, stale, or
broken instrumentation. The shared contract is owned by
[`stranske/Workflows#2150`](https://github.com/stranske/Workflows/issues/2150).

## Where records live

- Default artifact: `artifacts/langsmith/langsmith-fleet.ndjson` (gitignored).
- Override path: set `PENSION_DATA_LANGSMITH_FLEET_PATH=/absolute/path.ndjson`
  or pass `fleet_artifact_path=` to `run_nl_query_endpoint`.

Each NL request emits up to four lines — one per registry operation:
`sql-generation`, `validation`, `execution`, `replay`. The `replay` line is
emitted as `status="skipped"` unless replay metadata is supplied at call
time.

## Required domain fields

The Pension-Data registry entry mandates the following per-record domain
fields (validated against the v1 schema):

| Field | Source | Notes |
| --- | --- | --- |
| `query_category` | caller-provided | Coarse-grained NL intent (e.g. `funded_ratio_lookup`). |
| `sql_validation_status` | derived from `NLToSQLResponse` | `pass`, `unsafe`, `ambiguous`, `invalid_request`, or `unknown`. |
| `read_only_status` | derived from validation outcome | `read_only`, `blocked`, or `unknown`. |
| `row_count` | `NLToSQLMetadata.returned_rows` | Always sanitized to ≥0. |

Records also carry `stage` (matches the operation name), `latency_ms`,
`max_rows`, `trace_event_count`, and — when present — `trace_id`, `trace_url`,
`provider`, `model`, and `github_pr`. Raw prompts, generated SQL, member
data, and result rows are never written to the artifact.

## Status semantics

- `success` — `LANGSMITH_API_KEY` is set and the stage completed without
  error.
- `error` — the chain emitted a stage error (e.g. `UNSAFE_SQL`,
  `MAX_ROWS_EXCEEDED`). `error_category` carries the chain's error code.
- `skipped` — a later stage was not reached because an earlier stage failed
  (e.g. execution after a validation failure); or replay metadata was not
  provided.
- `no_secret` — `LANGSMITH_API_KEY` is unset. No network calls are made and
  the env vars are not modified.
- `fallback` — reserved for callers that swap to a deterministic backup
  chain; the NL route does not currently emit this status.

## Validating the artifact locally

The validator lives in Workflows:

```bash
git clone https://github.com/stranske/Workflows /tmp/wf
python /tmp/wf/scripts/langsmith_fleet.py \
  artifacts/langsmith/langsmith-fleet.ndjson \
  --summary --format markdown
```

It must accept the file. If it reports `invalid`, check that:

1. Every record carries the four required domain fields above.
2. `recorded_at` is set (the route fills this with a UTC ISO timestamp).
3. No raw SQL or prompt text leaked into the record (the helper does not
   write these fields; callers must not pass them via the `event` payload
   either).

## Wiring callers

`run_nl_query_endpoint(..., query_category="funded_ratio_lookup")` is the
production wiring point. Callers may also pass `fleet_trace_id`,
`fleet_trace_url`, and `fleet_github_pr` to thread LangSmith trace links and
PR context through. Bench/replay scripts can call
`pension_data.observability.langsmith_fleet.build_fleet_records_from_response`
directly and use `write_fleet_records` for one-off artifact writes.

## Disabling emission

Omit `query_category` (or pass empty string) and do not pass
`fleet_artifact_path`. The route then runs the NL chain normally and writes
nothing to the fleet artifact.
