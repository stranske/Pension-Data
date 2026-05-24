# NL Observability and Replay

This document describes the structured logging and replay tooling for Pension-Data NL operations.

## Structured Log Schema

NL requests are logged as JSONL rows with:
- `request_id`
- `correlation_id`
- `provider`
- `model`
- `question`
- `generated_sql`
- `status`
- `latency_ms`
- `returned_rows`
- `trace_event_count`
- `error_code`
- `error_message`
- `max_rows`
- `timeout_ms`

Default path:

`artifacts/langchain/nl_operations.jsonl`

## Retention Policy

Logs are trimmed to a bounded row count after each append.

Default retention limit:
- `2000` rows

## Replay Tool

Replay the latest logged request:

```bash
python scripts/langchain/nl_replay.py --db-path path/to/pension.db
```

Replay a specific request:

```bash
python scripts/langchain/nl_replay.py --db-path path/to/pension.db --request-id nlq:...
```

## Summary Tool

Compute lightweight failure/latency stats:

```bash
python scripts/langchain/nl_log_summary.py
```

## LangSmith Fleet Artifact

When the NL-to-SQL route is invoked with a `query_category` argument (e.g.
`funded_ratio_lookup`, `discount_rate_lookup`), the endpoint also appends
dashboard-safe records to the shared `langsmith-fleet/v1` NDJSON artifact:

`artifacts/langsmith/langsmith-fleet.ndjson`

Each NL run emits four records — one per registry operation
(`sql-generation`, `validation`, `execution`, `replay`) — with status,
validation outcome, read-only safety verdict, row count, latency, and stable
identifiers. Raw prompts, generated SQL, and row payloads are never written
to this artifact. Set `PENSION_DATA_LANGSMITH_FLEET_PATH` to override the
default path or pass `fleet_artifact_path=` to `run_nl_query_endpoint`. The
artifact is validated by `scripts/langsmith_fleet.py` in
[`stranske/Workflows#2150`](https://github.com/stranske/Workflows/issues/2150)
and rolled up into the fleet status dashboard.

When `LANGSMITH_API_KEY` is unset, the helper writes records with
`status="no_secret"` and performs no network calls; query behaviour is
unchanged.

