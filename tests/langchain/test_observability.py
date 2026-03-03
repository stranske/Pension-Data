"""Tests for NL observability logging, replay, and summary helpers."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from pension_data.api.auth import SCOPE_NL, APIKeyStore
from pension_data.api.routes.nl import run_nl_query_endpoint
from pension_data.langchain.nl_sql_chain import NLToSQLRequest, run_nl_sql_chain
from pension_data.langchain.observability import (
    NLOperationLogEntry,
    append_nl_operation_log,
    build_nl_operation_log_entry,
    default_nl_log_path,
    load_nl_operation_logs,
    replay_logged_request,
    summarize_nl_operation_logs,
)


class _StaticChain:
    def __init__(self, output: str | Mapping[str, Any]) -> None:
        self.output = output

    def invoke(self, values: Mapping[str, Any]) -> str | Mapping[str, Any]:
        del values
        return self.output


def _seed_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.execute(
        "CREATE TABLE sample_metrics (id INTEGER PRIMARY KEY, metric TEXT NOT NULL, value REAL NOT NULL)"
    )
    connection.executemany(
        "INSERT INTO sample_metrics (id, metric, value) VALUES (?, ?, ?)",
        [
            (1, "funded_ratio", 0.79),
            (2, "funded_ratio", 0.81),
            (3, "discount_rate", 0.0675),
        ],
    )
    return connection


def _sample_request() -> NLToSQLRequest:
    return NLToSQLRequest(
        question="Show funded ratio values by id",
        max_rows=10,
        timeout_ms=2_000,
    )


def _sample_entry(*, request_id: str, status: str, latency_ms: int) -> NLOperationLogEntry:
    return NLOperationLogEntry(
        timestamp="2026-03-03T00:00:00+00:00",
        request_id=request_id,
        correlation_id=request_id,
        provider="openai",
        model="gpt-4o-mini",
        question="Show funded ratio values by id",
        generated_sql="SELECT id, value FROM sample_metrics ORDER BY id",
        status="ok" if status == "ok" else "error",
        latency_ms=latency_ms,
        returned_rows=2,
        trace_event_count=3,
        error_code=None if status == "ok" else "UNSAFE_SQL",
        error_message=None if status == "ok" else "blocked",
        max_rows=10,
        timeout_ms=2_000,
    )


def test_append_log_enforces_retention(tmp_path: Path) -> None:
    log_path = tmp_path / "nl_ops.jsonl"
    append_nl_operation_log(
        path=log_path,
        entry=_sample_entry(request_id="r1", status="ok", latency_ms=100),
        retention_limit=2,
    )
    append_nl_operation_log(
        path=log_path,
        entry=_sample_entry(request_id="r2", status="error", latency_ms=200),
        retention_limit=2,
    )
    append_nl_operation_log(
        path=log_path,
        entry=_sample_entry(request_id="r3", status="ok", latency_ms=300),
        retention_limit=2,
    )
    entries = load_nl_operation_logs(log_path)
    assert tuple(entry.request_id for entry in entries) == ("r2", "r3")


def test_summarize_logs_reports_failure_and_latency() -> None:
    summary = summarize_nl_operation_logs(
        (
            _sample_entry(request_id="r1", status="ok", latency_ms=100),
            _sample_entry(request_id="r2", status="error", latency_ms=250),
            _sample_entry(request_id="r3", status="ok", latency_ms=150),
        )
    )
    assert summary.total_requests == 3
    assert summary.failed_requests == 1
    assert summary.avg_latency_ms > 0
    assert summary.p95_latency_ms >= 150


def test_replay_logged_request_uses_logged_sql() -> None:
    connection = _seed_connection()
    try:
        request = _sample_request()
        response = run_nl_sql_chain(
            connection=connection,
            request=request,
            chain=_StaticChain(
                "SELECT id, value FROM sample_metrics WHERE metric = 'funded_ratio'"
            ),
        )
        entry = build_nl_operation_log_entry(
            request=request,
            response=response,
            provider="openai",
            model="gpt-4o-mini",
            correlation_id="corr:test",
        )
        replayed = replay_logged_request(entry=entry, connection=connection)
    finally:
        connection.close()

    assert replayed.status == "ok"
    assert replayed.rows == response.rows
    assert replayed.sql == response.sql


def test_nl_route_emits_structured_log_entry(tmp_path: Path) -> None:
    key_store = APIKeyStore()
    secret, _ = key_store.create_key(scopes=(SCOPE_NL,))
    connection = _seed_connection()
    log_path = tmp_path / "nl_operations.jsonl"
    try:
        result = run_nl_query_endpoint(
            api_key_header=secret,
            key_store=key_store,
            connection=connection,
            request=_sample_request(),
            chain=_StaticChain(
                "SELECT id, value FROM sample_metrics WHERE metric = 'funded_ratio'"
            ),
            provider="openai",
            model="gpt-4o-mini",
            correlation_id="corr:unit-test",
            log_path=log_path,
            event={"request_origin": "unit-test"},
        )
    finally:
        connection.close()

    assert result.response.status == "ok"
    entries = load_nl_operation_logs(log_path)
    assert len(entries) == 1
    entry = entries[0]
    assert entry.request_id == result.response.metadata.request_id
    assert entry.correlation_id == "corr:unit-test"
    assert entry.provider == "openai"
    assert entry.model == "gpt-4o-mini"


def test_load_logs_skips_malformed_rows(tmp_path: Path) -> None:
    log_path = tmp_path / "nl_operations.jsonl"
    rows = [
        '{"timestamp":"2026-03-03T00:00:00+00:00","request_id":"ok1","correlation_id":"ok1","provider":"openai","model":"gpt-4o-mini","question":"q","generated_sql":"SELECT 1","status":"ok","latency_ms":10,"returned_rows":1,"trace_event_count":1,"error_code":null,"error_message":null,"max_rows":10,"timeout_ms":2000}',
        '{"timestamp":"2026-03-03T00:00:00+00:00","request_id":"bad-types","latency_ms":"NaN","returned_rows":"NaN"}',
        '{"broken_json":',
        '{"timestamp":"2026-03-03T00:00:00+00:00","request_id":"ok2","correlation_id":"ok2","provider":"openai","model":"gpt-4o-mini","question":"q","generated_sql":"SELECT 2","status":"error","latency_ms":20,"returned_rows":0,"trace_event_count":1,"error_code":"UNSAFE_SQL","error_message":"blocked","max_rows":10,"timeout_ms":2000}',
    ]
    log_path.write_text("\n".join(rows) + "\n", encoding="utf-8")

    entries = load_nl_operation_logs(log_path)
    assert tuple(entry.request_id for entry in entries) == ("ok1", "ok2")


def test_nl_route_logging_failure_does_not_break_endpoint(tmp_path: Path) -> None:
    key_store = APIKeyStore()
    secret, _ = key_store.create_key(scopes=(SCOPE_NL,))
    connection = _seed_connection()
    bad_log_path = tmp_path / "existing_dir"
    bad_log_path.mkdir(parents=True)
    try:
        result = run_nl_query_endpoint(
            api_key_header=secret,
            key_store=key_store,
            connection=connection,
            request=_sample_request(),
            chain=_StaticChain(
                "SELECT id, value FROM sample_metrics WHERE metric = 'funded_ratio'"
            ),
            provider="openai",
            model="gpt-4o-mini",
            log_path=bad_log_path,
        )
    finally:
        connection.close()

    assert result.response.status == "ok"
    assert result.audit_event["operation"] == "nl.ask"


def test_default_nl_log_path_respects_env_override(monkeypatch: Any, tmp_path: Path) -> None:
    override = tmp_path / "custom.jsonl"
    monkeypatch.setenv("PENSION_DATA_NL_LOG_PATH", str(override))
    assert default_nl_log_path() == override
