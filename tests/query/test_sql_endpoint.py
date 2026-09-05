"""Tests for audited SQL query endpoint behavior and error envelopes."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import fields, replace
from pathlib import Path
from typing import Any, Literal
from unittest.mock import Mock

import pytest

from pension_data.api.auth import SCOPE_EXPORT, SCOPE_QUERY, APIKeyStore, ScopeDeniedError
from pension_data.api.routes.sql import run_sql_query_endpoint
from pension_data.query.sql_service import (
    SQLExecutionAuditLog,
    SQLExecutionRunRecord,
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


def test_sql_endpoint_returns_standardized_success_envelope_and_audit_log(
    tmp_path: Path,
) -> None:
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
            run_record_root=tmp_path / "run-records",
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


def test_sql_execution_audit_log_shape_is_back_compat() -> None:
    assert tuple(field.name for field in fields(SQLExecutionAuditLog)) == (
        "query_id",
        "caller_key_id",
        "duration_ms",
        "row_count",
        "status",
        "error_code",
        "error_message",
    )


def test_sql_endpoint_writes_run_record_with_null_cost(tmp_path: Path) -> None:
    artifact_root = tmp_path
    key_store = APIKeyStore()
    secret, record = key_store.create_key(scopes=(SCOPE_QUERY,), label="analyst")
    connection = _seed_connection(rows=3)
    try:
        result = run_sql_query_endpoint(
            api_key_header=secret,
            key_store=key_store,
            connection=connection,
            request=SQLQueryRequest(
                sql="SELECT id, metric, value FROM sample_metrics ORDER BY id",
                page=1,
                page_size=2,
                max_rows=50,
            ),
            run_record_root=artifact_root,
            event={"correlation_id": "corr:sql-unit-test"},
        )
    finally:
        connection.close()

    record_path = next((artifact_root / "query" / "sql_runs" / "runs").glob("*.json"))
    payload = json.loads(record_path.read_text(encoding="utf-8"))
    assert payload["run_id"] == result.response.metadata.query_id
    assert payload["who"]["key_id"] == record.key_id
    assert payload["who"]["scopes"] == [SCOPE_QUERY]
    assert payload["who"]["required_scope"] == SCOPE_QUERY
    assert payload["who"]["correlation_id"] == "corr:sql-unit-test"
    assert result.response.metadata.executed_sql is not None
    assert result.response.metadata.executed_sql.startswith("SELECT * FROM (SELECT id, metric")
    assert payload["executed_sql"] == result.response.metadata.executed_sql
    assert payload["row_count"] == 2
    assert payload["rows_artifact"]["row_count"] == 2
    assert payload["rows_artifact"]["path"].startswith("query/sql_runs/rows/")
    assert payload["artifacts"][0]["path"] == payload["rows_artifact"]["path"]
    assert payload["cost"] is None


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
            request=SQLQueryRequest(
                sql="SELECT id FROM sample_metrics; SELECT value FROM sample_metrics"
            ),
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


def test_sql_service_persists_serialized_run_record(tmp_path: Path) -> None:
    connection = _seed_connection(rows=4)
    run_records: list[SQLExecutionRunRecord] = []
    try:
        response = execute_sql_query(
            connection=connection,
            request=SQLQueryRequest(
                sql="SELECT id, metric FROM sample_metrics ORDER BY id",
                page=1,
                page_size=2,
            ),
            caller_key_id="key:service-test",
            run_record_store=run_records,
            run_record_root=tmp_path,
        )
    finally:
        connection.close()

    assert response.status == "ok"
    assert len(run_records) == 1
    payload = run_records[0].to_dict()
    assert payload["run_id"] == response.metadata.query_id
    assert payload["who"]["key_id"] == "key:service-test"
    assert payload["columns"] == ["id", "metric"]
    assert payload["rows_artifact"]["path"].startswith("query/sql_runs/rows/")
    assert payload["provenance"] == []
    assert payload["record_artifact"]["path"].startswith("query/sql_runs/runs/")

    rows_path = tmp_path / payload["rows_artifact"]["path"]
    rows_payload = json.loads(rows_path.read_text(encoding="utf-8"))
    assert rows_payload["columns"] == ["id", "metric"]
    assert rows_payload["rows"] == [[1, "m-001"], [2, "m-002"]]


@pytest.mark.parametrize("field", ["page", "page_size", "timeout_ms", "max_rows"])
@pytest.mark.parametrize(
    "value", [float("nan"), float("inf"), float("-inf"), 1.5, 1.0, True, False, "1", None]
)
@pytest.mark.parametrize("dialect", ["sqlite", "postgresql"])
def test_sql_service_rejects_non_finite_resource_controls(
    field: str,
    value: Any,
    dialect: Literal["sqlite", "postgresql"],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = Mock(spec=["execute", "set_progress_handler"])
    validate_sql = Mock(side_effect=AssertionError("SQL validation must not run"))
    monkeypatch.setattr("pension_data.query.sql_service.validate_read_only_sql", validate_sql)
    request = replace(SQLQueryRequest(sql="SELECT 1"), **{field: value})
    audit_logs: list[SQLExecutionAuditLog] = []

    response = execute_sql_query(
        connection=connection,
        request=request,
        dialect=dialect,
        caller_key_id="key:test",
        audit_log_store=audit_logs,
    )

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "INVALID_REQUEST"
    assert response.error.message == f"{field} must be a finite integer"
    assert response.rows == ()
    assert response.metadata.executed_sql is None
    assert audit_logs[0].error_code == "INVALID_REQUEST"
    validate_sql.assert_not_called()
    assert connection.method_calls == []


@pytest.mark.parametrize("field", ["page", "page_size", "timeout_ms", "max_rows"])
@pytest.mark.parametrize("value", [0, -1])
def test_sql_service_preserves_resource_control_lower_bounds(field: str, value: int) -> None:
    connection = Mock(spec=["execute", "set_progress_handler"])
    overrides: dict[str, Any] = {field: value}
    response = execute_sql_query(
        connection=connection,
        request=replace(SQLQueryRequest(sql="SELECT 1"), **overrides),
        caller_key_id="key:test",
    )
    assert response.error is not None
    assert response.error.code == "INVALID_REQUEST"
    assert response.error.message == f"{field} must be >= 1"
    assert connection.method_calls == []


@pytest.mark.parametrize("dialect", ["sqlite", "postgresql"])
@pytest.mark.parametrize(
    "invalid_request",
    [
        SQLQueryRequest(sql="SELECT 1; SELECT 2"),
        SQLQueryRequest(sql="EXPLAIN SELECT 1"),
        SQLQueryRequest(sql="SELECT 1", params={"_pd_limit": 100}),
        SQLQueryRequest(sql="SELECT 1", params="invalid"),  # type: ignore[arg-type]
    ],
    ids=["multiple-statements", "explain", "reserved-params", "string-params"],
)
def test_sql_service_validation_leaves_connection_untouched(
    invalid_request: SQLQueryRequest,
    dialect: Literal["sqlite", "postgresql"],
) -> None:
    methods = ["execute", "rollback"]
    if dialect == "sqlite":
        methods.append("set_progress_handler")
    connection = Mock(spec=methods)

    response = execute_sql_query(
        connection=connection,
        request=invalid_request,
        dialect=dialect,
        caller_key_id="key:test",
    )

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "INVALID_REQUEST"
    assert connection.method_calls == []


@pytest.mark.parametrize("dialect", ["sqlite", "postgresql"])
@pytest.mark.parametrize("execution_error", [False, True])
def test_sql_service_clears_installed_timeout_after_execution(
    dialect: Literal["sqlite", "postgresql"],
    execution_error: bool,
) -> None:
    methods = ["execute"]
    if dialect == "sqlite":
        methods.append("set_progress_handler")
    connection = Mock(spec=methods)
    count_cursor = Mock()
    count_cursor.fetchone.return_value = (1,)
    page_cursor = Mock()
    page_cursor.description = (("value",),)
    page_cursor.fetchall.return_value = [(1,)]
    results: list[Any] = [
        RuntimeError("query failed") if execution_error else count_cursor,
    ]
    if not execution_error:
        results.append(page_cursor)
    if dialect == "postgresql":
        results = [None, *results, None]
    connection.execute.side_effect = results

    response = execute_sql_query(
        connection=connection,
        request=SQLQueryRequest(sql="SELECT 1 AS value", timeout_ms=1234),
        dialect=dialect,
        caller_key_id="key:test",
    )

    assert response.status == ("error" if execution_error else "ok")
    if execution_error:
        assert response.error is not None
        assert response.error.message == "query failed"
    else:
        assert response.rows == ((1,),)
    if dialect == "sqlite":
        assert connection.set_progress_handler.call_count == 2
        assert callable(connection.set_progress_handler.call_args_list[0].args[0])
        assert connection.set_progress_handler.call_args_list[0].args[1] == 1000
        connection.set_progress_handler.assert_called_with(None, 0)
        assert connection.method_calls[-1][0] == "set_progress_handler"
    else:
        assert connection.execute.call_args_list[0].args == ("SET statement_timeout = 1234",)
        connection.execute.assert_called_with("SET statement_timeout = DEFAULT")


@pytest.mark.parametrize("dialect", ["sqlite", "postgresql"])
def test_sql_service_does_not_clear_timeout_when_installation_fails(
    dialect: Literal["sqlite", "postgresql"],
) -> None:
    methods = ["execute", "rollback"]
    if dialect == "sqlite":
        methods.append("set_progress_handler")
    connection = Mock(spec=methods)
    setter = connection.set_progress_handler if dialect == "sqlite" else connection.execute
    setter.side_effect = RuntimeError("timeout installation failed")

    response = execute_sql_query(
        connection=connection,
        request=SQLQueryRequest(sql="SELECT 1"),
        dialect=dialect,
        caller_key_id="key:test",
    )

    assert response.status == "error"
    assert response.error is not None
    assert response.error.message == "timeout installation failed"
    assert len(connection.method_calls) == 1
    setter.assert_called_once()
