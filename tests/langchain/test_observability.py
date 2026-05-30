"""Tests for NL observability logging, replay, and summary helpers."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import fields
from pathlib import Path
from typing import Any

from pension_data.api.auth import SCOPE_NL, APIKeyStore
from pension_data.api.routes.nl import run_nl_query_endpoint
from pension_data.langchain.nl_sql_chain import NLToSQLRequest, run_nl_sql_chain
from pension_data.langchain.observability import (
    NLOperationLogEntry,
    append_nl_operation_log,
    build_nl_operation_log_entry,
    build_nl_query_run_record,
    default_nl_log_path,
    load_nl_operation_logs,
    persist_nl_query_run_record,
    replay_logged_request,
    replay_run_record,
    summarize_nl_operation_logs,
)
from pension_data.query.run_record import QueryRunArtifact
from pension_data.query.sql_safety import SQLSafetyPolicy


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


def _sample_policy() -> SQLSafetyPolicy:
    return SQLSafetyPolicy(
        allowed_relations=("sample_metrics",),
        allowed_columns=("id", "metric", "value"),
        banned_clauses=(),
        max_rows=10,
        max_timeout_ms=2_000,
    )


def _provenance_policy() -> SQLSafetyPolicy:
    return SQLSafetyPolicy(
        allowed_relations=("sample_metrics",),
        allowed_columns=(
            "id",
            "metric",
            "value",
            "source_document_id",
            "evidence_refs",
            "confidence",
        ),
        banned_clauses=(),
        max_rows=10,
        max_timeout_ms=2_000,
        require_source_document_id=True,
    )


def _seed_provenance_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.execute("""
        CREATE TABLE sample_metrics (
            id INTEGER PRIMARY KEY,
            metric TEXT NOT NULL,
            value REAL NOT NULL,
            source_document_id TEXT NOT NULL,
            evidence_refs TEXT NOT NULL,
            confidence REAL NOT NULL
        )
        """)
    connection.executemany(
        """
        INSERT INTO sample_metrics (
            id, metric, value, source_document_id, evidence_refs, confidence
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (1, "funded_ratio", 0.79, "doc-001", "page:3,row:12", 0.91),
            (2, "funded_ratio", 0.81, "doc-002", "page:4,row:6", 0.94),
        ],
    )
    return connection


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


def test_nl_operation_log_entry_shape_is_back_compat() -> None:
    assert tuple(field.name for field in fields(NLOperationLogEntry)) == (
        "timestamp",
        "request_id",
        "correlation_id",
        "provider",
        "model",
        "question",
        "generated_sql",
        "status",
        "latency_ms",
        "returned_rows",
        "trace_event_count",
        "error_code",
        "error_message",
        "max_rows",
        "timeout_ms",
    )


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
            policy=_sample_policy(),
        )
        entry = build_nl_operation_log_entry(
            request=request,
            response=response,
            provider="openai",
            model="gpt-4o-mini",
            correlation_id="corr:test",
        )
        replayed = replay_logged_request(
            entry=entry,
            connection=connection,
            policy=_sample_policy(),
        )
    finally:
        connection.close()

    assert replayed.status == "ok"
    assert replayed.rows == response.rows
    assert replayed.sql == response.sql


def test_nl_query_run_record_captures_full_run_and_is_byte_stable() -> None:
    connection = _seed_provenance_connection()
    try:
        request = _sample_request()
        response = run_nl_sql_chain(
            connection=connection,
            request=request,
            chain=_StaticChain(
                {
                    "sql": (
                        "SELECT id, value, source_document_id, evidence_refs, confidence "
                        "FROM sample_metrics WHERE metric = 'funded_ratio' ORDER BY id"
                    ),
                    "usage": {
                        "prompt_tokens": 41,
                        "completion_tokens": 12,
                        "total_tokens": 53,
                        "cost_usd": 0.00031,
                    },
                }
            ),
            policy=_provenance_policy(),
        )
    finally:
        connection.close()

    rows_artifact = QueryRunArtifact(
        name="nl-query-rows",
        path="langchain/nl_runs/rows/fixed.json",
        content_type="application/json",
        row_count=response.metadata.returned_rows,
    )
    record = build_nl_query_run_record(
        request=request,
        response=response,
        key_id="key:test",
        scopes=(SCOPE_NL,),
        required_scope=SCOPE_NL,
        correlation_id="corr:test",
        rows_artifact=rows_artifact,
    )
    rebuilt = build_nl_query_run_record(
        request=request,
        response=response,
        key_id="key:test",
        scopes=(SCOPE_NL,),
        required_scope=SCOPE_NL,
        correlation_id="corr:test",
        rows_artifact=rows_artifact,
    )

    payload = record.to_dict()
    assert payload["who"] == {
        "key_id": "key:test",
        "scopes": [SCOPE_NL],
        "required_scope": SCOPE_NL,
        "correlation_id": "corr:test",
    }
    assert payload["inputs"]["question"] == "Show funded ratio values by id"
    assert payload["generated_sql"] == response.sql
    assert payload["rows_artifact"]["path"] == "langchain/nl_runs/rows/fixed.json"
    assert payload["provenance"][0]["source_document_id"] == "doc-001"
    assert payload["warnings"] == []
    assert payload["error"] is None
    assert payload["duration_ms"] >= 0
    assert payload["cost"] == {
        "prompt_tokens": 41,
        "completion_tokens": 12,
        "total_tokens": 53,
        "cost_usd": 0.00031,
    }
    assert json.dumps(payload, sort_keys=True) == json.dumps(rebuilt.to_dict(), sort_keys=True)


def test_replay_run_record_reconstructs_rows_and_provenance(tmp_path: Path) -> None:
    connection = _seed_provenance_connection()
    try:
        request = _sample_request()
        response = run_nl_sql_chain(
            connection=connection,
            request=request,
            chain=_StaticChain(
                {
                    "sql": (
                        "SELECT id, value, source_document_id, evidence_refs, confidence "
                        "FROM sample_metrics WHERE metric = 'funded_ratio' ORDER BY id"
                    ),
                    "token_usage": {"input_tokens": 20, "output_tokens": 5},
                }
            ),
            policy=_provenance_policy(),
        )
    finally:
        connection.close()

    persist_nl_query_run_record(
        request=request,
        response=response,
        key_id="key:test",
        scopes=(SCOPE_NL,),
        required_scope=SCOPE_NL,
        correlation_id="corr:test",
        root=tmp_path,
    )
    record_path = next((tmp_path / "langchain" / "nl_runs" / "runs").glob("*.json"))
    record = json.loads(record_path.read_text(encoding="utf-8"))
    replayed = replay_run_record(record=record, root=tmp_path)

    assert replayed.rows == response.rows
    assert replayed.provenance == response.provenance
    assert replayed.metadata.cost == {
        "prompt_tokens": 20,
        "completion_tokens": 5,
        "total_tokens": 25,
        "cost_usd": None,
    }


def test_nl_error_run_record_preserves_provider_usage_metadata(tmp_path: Path) -> None:
    key_store = APIKeyStore()
    secret, _ = key_store.create_key(scopes=(SCOPE_NL,), label="analyst")
    connection = _seed_connection()
    try:
        result = run_nl_query_endpoint(
            api_key_header=secret,
            key_store=key_store,
            connection=connection,
            request=_sample_request(),
            chain=_StaticChain(
                {
                    "sql": "DELETE FROM sample_metrics WHERE id = 1",
                    "usage": {"prompt_tokens": 9, "completion_tokens": 3, "cost_usd": "0.0002"},
                }
            ),
            policy=_sample_policy(),
            run_record_root=tmp_path,
        )
    finally:
        connection.close()

    assert result.response.status == "error"
    assert result.response.metadata.cost == {
        "prompt_tokens": 9,
        "completion_tokens": 3,
        "total_tokens": 12,
        "cost_usd": 0.0002,
    }

    record_path = next((tmp_path / "langchain" / "nl_runs" / "runs").glob("*.json"))
    payload = json.loads(record_path.read_text(encoding="utf-8"))
    assert payload["status"] == "error"
    assert payload["cost"] == {
        "prompt_tokens": 9,
        "completion_tokens": 3,
        "total_tokens": 12,
        "cost_usd": 0.0002,
    }


def test_nl_query_run_record_preserves_zero_usage_values(tmp_path: Path) -> None:
    key_store = APIKeyStore()
    secret, _ = key_store.create_key(scopes=(SCOPE_NL,), label="analyst")
    connection = _seed_connection()
    try:
        result = run_nl_query_endpoint(
            api_key_header=secret,
            key_store=key_store,
            connection=connection,
            request=_sample_request(),
            chain=_StaticChain(
                {
                    "sql": "SELECT id, value FROM sample_metrics WHERE metric = 'funded_ratio'",
                    "usage": {
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "total_tokens": 0,
                        "cost_usd": 0,
                    },
                }
            ),
            policy=_sample_policy(),
            run_record_root=tmp_path,
        )
    finally:
        connection.close()

    assert result.response.status == "ok"
    assert result.response.metadata.cost == {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "cost_usd": 0.0,
    }

    record_path = next((tmp_path / "langchain" / "nl_runs" / "runs").glob("*.json"))
    payload = json.loads(record_path.read_text(encoding="utf-8"))
    assert payload["cost"] == {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "cost_usd": 0.0,
    }


def test_nl_route_persists_nested_provider_usage_and_replayable_rows_artifact(
    tmp_path: Path,
) -> None:
    key_store = APIKeyStore()
    secret, _ = key_store.create_key(scopes=(SCOPE_NL,), label="analyst")
    connection = _seed_provenance_connection()
    try:
        result = run_nl_query_endpoint(
            api_key_header=secret,
            key_store=key_store,
            connection=connection,
            request=NLToSQLRequest(
                question="Show funded ratio sources",
                max_rows=10,
                timeout_ms=2_000,
            ),
            chain=_StaticChain(
                {
                    "sql": (
                        "SELECT id, value, source_document_id, evidence_refs, confidence "
                        "FROM sample_metrics WHERE metric = 'funded_ratio' ORDER BY id"
                    ),
                    "response_metadata": {
                        "token_usage": {
                            "prompt_tokens": 17,
                            "completion_tokens": 6,
                            "total_tokens": 23,
                            "cost_usd": "0.00044",
                        }
                    },
                }
            ),
            policy=_provenance_policy(),
            run_record_root=tmp_path,
        )
    finally:
        connection.close()

    assert result.response.status == "ok"
    assert result.response.metadata.cost == {
        "prompt_tokens": 17,
        "completion_tokens": 6,
        "total_tokens": 23,
        "cost_usd": 0.00044,
    }

    record_path = next((tmp_path / "langchain" / "nl_runs" / "runs").glob("*.json"))
    payload = json.loads(record_path.read_text(encoding="utf-8"))
    assert payload["cost"] == {
        "prompt_tokens": 17,
        "completion_tokens": 6,
        "total_tokens": 23,
        "cost_usd": 0.00044,
    }
    replayed = replay_run_record(record=payload, root=tmp_path)
    assert replayed.rows == result.response.rows
    assert replayed.provenance == result.response.provenance


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
            run_record_root=tmp_path / "run-records",
            event={"request_origin": "unit-test"},
            policy=_sample_policy(),
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
            run_record_root=tmp_path / "run-records",
            policy=_sample_policy(),
        )
    finally:
        connection.close()

    assert result.response.status == "ok"
    assert result.audit_event["operation"] == "nl.ask"


def test_default_nl_log_path_respects_env_override(monkeypatch: Any, tmp_path: Path) -> None:
    override = tmp_path / "custom.jsonl"
    monkeypatch.setenv("PENSION_DATA_NL_LOG_PATH", str(override))
    assert default_nl_log_path() == override
