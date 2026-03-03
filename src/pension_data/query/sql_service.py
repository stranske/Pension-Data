"""Audited SQL execution service with deterministic response envelopes."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Literal
from uuid import uuid4

SqlParams = Mapping[str, Any] | Sequence[Any]

_FORBIDDEN_SQL_TOKENS: tuple[str, ...] = (
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "replace",
    "attach",
    "detach",
    "pragma",
    "vacuum",
    "reindex",
)


@dataclass(frozen=True, slots=True)
class SQLQueryRequest:
    """SQL request contract with pagination and resource guardrails."""

    sql: str
    params: SqlParams | None = None
    page: int = 1
    page_size: int = 100
    timeout_ms: int = 2_000
    max_rows: int = 1_000


@dataclass(frozen=True, slots=True)
class SQLQueryError:
    """Stable SQL error envelope."""

    code: str
    message: str


@dataclass(frozen=True, slots=True)
class SQLQueryMetadata:
    """Execution metadata included for successful and failed responses."""

    query_id: str
    page: int
    page_size: int
    returned_rows: int
    has_more: bool
    total_rows: int | None
    duration_ms: int


@dataclass(frozen=True, slots=True)
class SQLQueryResponse:
    """Deterministic response envelope for SQL endpoint execution."""

    status: Literal["ok", "error"]
    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]
    metadata: SQLQueryMetadata
    error: SQLQueryError | None


@dataclass(frozen=True, slots=True)
class SQLExecutionAuditLog:
    """Audited query metadata for success and error outcomes."""

    query_id: str
    caller_key_id: str
    duration_ms: int
    row_count: int
    status: Literal["ok", "error"]
    error_code: str | None
    error_message: str | None


class SQLExecutionValidationError(ValueError):
    """Raised for SQL policy or request validation failures."""


class SQLRowLimitExceededError(SQLExecutionValidationError):
    """Raised when query result would exceed configured max_rows guardrail."""


def _normalized_sql(sql: str) -> str:
    normalized = sql.strip().rstrip(";").strip()
    if not normalized:
        raise SQLExecutionValidationError("SQL query must be non-empty")
    return normalized


def _validate_request(request: SQLQueryRequest) -> None:
    if request.page < 1:
        raise SQLExecutionValidationError("page must be >= 1")
    if request.page_size < 1:
        raise SQLExecutionValidationError("page_size must be >= 1")
    if request.timeout_ms < 1:
        raise SQLExecutionValidationError("timeout_ms must be >= 1")
    if request.max_rows < 1:
        raise SQLExecutionValidationError("max_rows must be >= 1")


def _enforce_read_only_sql(sql: str) -> None:
    lowered = sql.lower().lstrip()
    if not lowered.startswith(("select", "with", "explain")):
        raise SQLExecutionValidationError("only read-only SELECT/WITH/EXPLAIN queries are allowed")
    tokens = {token.strip("(),") for token in lowered.replace("\n", " ").split()}
    forbidden = sorted(token for token in _FORBIDDEN_SQL_TOKENS if token in tokens)
    if forbidden:
        raise SQLExecutionValidationError(
            "query contains forbidden token(s): " + ", ".join(forbidden)
        )


def _count_params(params: SqlParams | None) -> SqlParams | tuple[()]:
    return params if params is not None else ()


def _paged_params(params: SqlParams | None, *, limit: int, offset: int) -> SqlParams:
    if params is None:
        return (limit, offset)
    if isinstance(params, Mapping):
        updated = dict(params)
        updated["_pd_limit"] = limit
        updated["_pd_offset"] = offset
        return updated
    return (*params, limit, offset)


def _count_query(sql: str) -> str:
    return f"SELECT COUNT(*) AS _pd_total_rows FROM ({sql}) AS _pd_count"


def _paged_query(sql: str, *, params: SqlParams | None) -> str:
    if isinstance(params, Mapping):
        return f"SELECT * FROM ({sql}) AS _pd_page LIMIT :_pd_limit OFFSET :_pd_offset"
    return f"SELECT * FROM ({sql}) AS _pd_page LIMIT ? OFFSET ?"


def _error_code(exc: Exception) -> str:
    if isinstance(exc, SQLRowLimitExceededError):
        return "ROW_LIMIT_EXCEEDED"
    if isinstance(exc, SQLExecutionValidationError):
        return "INVALID_REQUEST"
    if isinstance(exc, TimeoutError):
        return "TIMEOUT"
    if isinstance(exc, sqlite3.OperationalError):
        message = str(exc).lower()
        if "syntax error" in message:
            return "SYNTAX_ERROR"
        if "interrupted" in message:
            return "TIMEOUT"
    return "EXECUTION_ERROR"


def _set_timeout_handler(
    connection: sqlite3.Connection,
    *,
    deadline_s: float,
    clock: Callable[[], float],
) -> None:
    def _check_timeout() -> int:
        return 1 if clock() > deadline_s else 0

    connection.set_progress_handler(_check_timeout, 1_000)


def _clear_timeout_handler(connection: sqlite3.Connection) -> None:
    connection.set_progress_handler(None, 0)


def execute_sql_query(
    *,
    connection: sqlite3.Connection,
    request: SQLQueryRequest,
    caller_key_id: str,
    audit_log_store: list[SQLExecutionAuditLog] | None = None,
    clock: Callable[[], float] = perf_counter,
) -> SQLQueryResponse:
    """Execute one SQL request with auditing, guardrails, and stable envelopes."""
    query_id = f"query:{uuid4().hex}"
    start = clock()

    def _finalize(
        *,
        status: Literal["ok", "error"],
        columns: tuple[str, ...],
        rows: tuple[tuple[Any, ...], ...],
        total_rows: int | None,
        error: SQLQueryError | None,
    ) -> SQLQueryResponse:
        duration_ms = max(0, int(round((clock() - start) * 1000)))
        metadata = SQLQueryMetadata(
            query_id=query_id,
            page=request.page,
            page_size=request.page_size,
            returned_rows=len(rows),
            has_more=(total_rows is not None and (request.page * request.page_size) < total_rows),
            total_rows=total_rows,
            duration_ms=duration_ms,
        )
        response = SQLQueryResponse(
            status=status,
            columns=columns,
            rows=rows,
            metadata=metadata,
            error=error,
        )
        if audit_log_store is not None:
            audit_log_store.append(
                SQLExecutionAuditLog(
                    query_id=query_id,
                    caller_key_id=caller_key_id,
                    duration_ms=duration_ms,
                    row_count=len(rows),
                    status=status,
                    error_code=error.code if error is not None else None,
                    error_message=error.message if error is not None else None,
                )
            )
        return response

    try:
        _validate_request(request)
        sql = _normalized_sql(request.sql)
        _enforce_read_only_sql(sql)

        deadline = start + (request.timeout_ms / 1000.0)
        _set_timeout_handler(connection, deadline_s=deadline, clock=clock)

        count_cursor = connection.execute(_count_query(sql), _count_params(request.params))
        total_rows = int(count_cursor.fetchone()[0])
        if total_rows > request.max_rows:
            raise SQLRowLimitExceededError(
                f"query result exceeds max_rows limit ({request.max_rows})"
            )

        offset = (request.page - 1) * request.page_size
        if offset >= total_rows:
            return _finalize(
                status="ok",
                columns=(),
                rows=(),
                total_rows=total_rows,
                error=None,
            )

        page_limit = min(request.page_size, request.max_rows)
        page_cursor = connection.execute(
            _paged_query(sql, params=request.params),
            _paged_params(request.params, limit=page_limit, offset=offset),
        )
        columns = tuple(column[0] for column in (page_cursor.description or ()))
        rows = tuple(tuple(row) for row in page_cursor.fetchall())
        return _finalize(
            status="ok",
            columns=columns,
            rows=rows,
            total_rows=total_rows,
            error=None,
        )
    except Exception as exc:  # noqa: BLE001
        if isinstance(exc, sqlite3.OperationalError) and "interrupted" in str(exc).lower():
            exc = TimeoutError("query timed out before completion")
        error = SQLQueryError(code=_error_code(exc), message=str(exc))
        return _finalize(
            status="error",
            columns=(),
            rows=(),
            total_rows=None,
            error=error,
        )
    finally:
        _clear_timeout_handler(connection)
