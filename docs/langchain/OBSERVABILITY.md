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

