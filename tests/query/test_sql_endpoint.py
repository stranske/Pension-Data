"""Tests for audited SQL query endpoint behavior and error envelopes."""

from __future__ import annotations

import sqlite3

import pytest

from pension_data.api.auth import SCOPE_EXPORT, SCOPE_QUERY, APIKeyStore, ScopeDeniedError
from pension_data.api.routes.sql import run_sql_query_endpoint
from pension_data.query.sql_service import (
    SQLExecutionAuditLog,
    SQLQueryRequest,
    execute_sql_query,
)


def _seed_connection(*, rows: int = 5) -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.execute(
        "CREATE TABLE sample_metrics (id INTEGER PRIMARY KEY, metric TEXT NOT NULL, value REAL NOT NULL)"
    )
    connection.executemany(
        "INSERT INTO sample_metrics (id, metric, value) VALUES (?, ?, ?)",
        [(index, f"m-{index:03d}", float(index)) for index in range(1, rows + 1)],
    )
    return connection


def test_sql_endpoint_returns_standardized_success_envelope_and_audit_log() -> None:
    key_store = APIKeyStore()
    secret, record = key_store.create_key(scopes=(SCOPE_QUERY,), label="analyst")
    connection = _seed_connection(rows=5)
    audit_logs: list[SQLExecutionAuditLog] = []
    try:
        result = run_sql_query_endpoint(
            api_key_header=secret,
            key_store=key_store,
            connection=connection,
            request=SQLQueryRequest(
                sql="SELECT id, metric, value FROM sample_metrics ORDER BY id",
                page=2,
                page_size=2,
                max_rows=50,
            ),
            audit_log_store=audit_logs,
        )
    finally:
        connection.close()

    assert result.response.status == "ok"
    assert result.response.columns == ("id", "metric", "value")
    assert result.response.rows == ((3, "m-003", 3.0), (4, "m-004", 4.0))
    assert result.response.metadata.page == 2
    assert result.response.metadata.page_size == 2
    assert result.response.metadata.returned_rows == 2
    assert result.response.metadata.total_rows == 5
    assert result.response.metadata.has_more is True
    assert result.response.error is None

    assert len(audit_logs) == 1
    assert audit_logs[0].caller_key_id == record.key_id
    assert audit_logs[0].status == "ok"
    assert audit_logs[0].row_count == 2
    assert result.audit_event["operation"] == "query.run"
    assert result.audit_event["api_key_id"] == record.key_id
    assert result.audit_event["query_status"] == "ok"


def test_sql_endpoint_rejects_unauthorized_scope() -> None:
    key_store = APIKeyStore()
    secret, _ = key_store.create_key(scopes=(SCOPE_EXPORT,))
    connection = _seed_connection(rows=3)
    try:
        with pytest.raises(ScopeDeniedError):
            run_sql_query_endpoint(
                api_key_header=secret,
                key_store=key_store,
                connection=connection,
                request=SQLQueryRequest(sql="SELECT id FROM sample_metrics"),
            )
    finally:
        connection.close()


def test_sql_service_returns_stable_error_schema_for_syntax_errors() -> None:
    connection = _seed_connection(rows=3)
    audit_logs: list[SQLExecutionAuditLog] = []
    try:
        response = execute_sql_query(
            connection=connection,
            request=SQLQueryRequest(sql="SELECT FROM sample_metrics"),
            caller_key_id="key:test",
            audit_log_store=audit_logs,
        )
    finally:
        connection.close()

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "SYNTAX_ERROR"
    assert response.columns == ()
    assert response.rows == ()
    assert response.metadata.returned_rows == 0
    assert response.metadata.total_rows is None
    assert len(audit_logs) == 1
    assert audit_logs[0].status == "error"
    assert audit_logs[0].error_code == "SYNTAX_ERROR"


def test_sql_service_times_out_long_running_query() -> None:
    connection = _seed_connection(rows=1)
    audit_logs: list[SQLExecutionAuditLog] = []

    tick = {"value": 0.0}

    def _clock() -> float:
        tick["value"] += 0.01
        return tick["value"]

    sql = """
        WITH RECURSIVE seq(x) AS (
            SELECT 1
            UNION ALL
            SELECT x + 1 FROM seq WHERE x < 500000
        )
        SELECT x FROM seq
    """

    try:
        response = execute_sql_query(
            connection=connection,
            request=SQLQueryRequest(sql=sql, timeout_ms=5),
            caller_key_id="key:test",
            audit_log_store=audit_logs,
            clock=_clock,
        )
    finally:
        connection.close()

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "TIMEOUT"
    assert len(audit_logs) == 1
    assert audit_logs[0].error_code == "TIMEOUT"


def test_sql_service_enforces_max_rows_limit() -> None:
    connection = _seed_connection(rows=200)
    audit_logs: list[SQLExecutionAuditLog] = []
    try:
        response = execute_sql_query(
            connection=connection,
            request=SQLQueryRequest(
                sql="SELECT id, metric FROM sample_metrics ORDER BY id",
                page=1,
                page_size=25,
                max_rows=50,
            ),
            caller_key_id="key:test",
            audit_log_store=audit_logs,
        )
    finally:
        connection.close()

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "ROW_LIMIT_EXCEEDED"
    assert "max_rows limit" in response.error.message
    assert len(audit_logs) == 1
    assert audit_logs[0].status == "error"
    assert audit_logs[0].error_code == "ROW_LIMIT_EXCEEDED"


def test_sql_service_rejects_statement_separator_before_execution() -> None:
    connection = _seed_connection(rows=2)
    try:
        response = execute_sql_query(
            connection=connection,
            request=SQLQueryRequest(sql="SELECT id FROM sample_metrics; SELECT value FROM sample_metrics"),
            caller_key_id="key:test",
        )
    finally:
        connection.close()

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "INVALID_REQUEST"
    assert "multiple SQL statements" in response.error.message


def test_sql_service_rejects_reserved_paging_keys_in_named_params() -> None:
    connection = _seed_connection(rows=2)
    try:
        response = execute_sql_query(
            connection=connection,
            request=SQLQueryRequest(
                sql="SELECT id FROM sample_metrics WHERE id >= :min_id",
                params={"min_id": 1, "_pd_limit": 100},
            ),
            caller_key_id="key:test",
        )
    finally:
        connection.close()

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "INVALID_REQUEST"
    assert "reserved paging key" in response.error.message


def test_sql_service_rejects_string_params_payload() -> None:
    connection = _seed_connection(rows=2)
    try:
        response = execute_sql_query(
            connection=connection,
            request=SQLQueryRequest(
                sql="SELECT id FROM sample_metrics WHERE metric = ?",
                params="m-001",  # type: ignore[arg-type]
            ),
            caller_key_id="key:test",
        )
    finally:
        connection.close()

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "INVALID_REQUEST"
    assert "mapping or positional list/tuple" in response.error.message


def test_sql_service_rejects_explain_queries_with_paging_wrapper() -> None:
    connection = _seed_connection(rows=2)
    try:
        response = execute_sql_query(
            connection=connection,
            request=SQLQueryRequest(sql="EXPLAIN SELECT id FROM sample_metrics"),
            caller_key_id="key:test",
        )
    finally:
        connection.close()

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "INVALID_REQUEST"
    assert "SELECT/WITH" in response.error.message
