# Issue #957 closer repair evidence (repair-of #980)

Check date: 2026-11-03  
Base: `origin/main` at `aba8f68` (includes merged PR #980).

## Temporal validation vs `_ingestion_instant`

Inspected `src/pension_data/api/artifact_data.py`:

- `_metric_history_row` validates `ingestion_date` (and related temporal fields) with `_validate_temporal` before rows enter `load_query_data_snapshot`.
- `load_query_data_snapshot` builds `MetricHistoryRow` tuples, then calls `_build_funding_trend_projection`, which resolves versions via `_resolve_metric_versions` using `_ingestion_instant(version.ingestion_date)`.
- Flow disclosure rows validate `reported_at` with `_validate_temporal` in `_validate_flow_row` before those values become `FundingTrendMetricVersion.ingestion_date`.

Both helpers parse with `datetime.fromisoformat(value.replace("Z", "+00:00"))`. Spot checks (date-only, `Z`, naive, offset-aware, whitespace-padded, invalid text) showed identical accept/reject behavior; no production change was required.

## Three-suite acceptance

See `acceptance-three-suite.txt` for command, interpreter, exit status, and pytest output (124 passed; includes unchanged `test_load_query_data_snapshot_rejects_conflicting_duplicate_fact_id`).

## Deliberate-break gate (cross-document conflict handling)

Temporarily replaced the `period_conflicts` branch in `_build_funding_trend_projection` with the pre-#980 fatal raise:

`QueryArtifactError: ambiguous {metric_target} for {plan_id} {plan_period}`

- Disconnected: `gate-cross-document-conflict-fail.txt` — `test_load_query_data_snapshot_keeps_revised_rows_and_marks_cross_document_conflict` failed at snapshot load (exit 1).
- Restored production source byte-for-byte: `gate-cross-document-conflict-pass.txt` — same test passed (exit 0).
