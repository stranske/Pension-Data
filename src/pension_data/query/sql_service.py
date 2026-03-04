"""Audited SQL execution service with deterministic response envelopes."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Literal, Protocol, cast
from uuid import uuid4

DatabaseDialect = Literal["sqlite", "postgresql"]
SqlParams = Mapping[str, Any] | tuple[Any, ...] | list[Any]

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
_RESERVED_PAGING_PARAM_KEYS: frozenset[str] = frozenset({"_pd_limit", "_pd_offset"})
_SQL_WORD_PATTERN = re.compile(r"\b[a-z_][a-z0-9_]*\b", re.IGNORECASE)


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


class DBConnection(Protocol):
    """Minimal DB-API connection contract used by SQL service."""

    def execute(self, sql: str, params: SqlParams | tuple[()] = ()) -> Any:
        """Execute SQL and return DB-API cursor-like result."""


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
    sanitized = _strip_sql_comments_and_strings(sql)
    lowered = sanitized.lower().lstrip()
    if not lowered.startswith(("select", "with")):
        raise SQLExecutionValidationError("only read-only SELECT/WITH queries are allowed")
    if ";" in sanitized:
        raise SQLExecutionValidationError("multiple SQL statements are not allowed")
    tokens = {match.group(0).lower() for match in _SQL_WORD_PATTERN.finditer(sanitized)}
    forbidden = sorted(token for token in _FORBIDDEN_SQL_TOKENS if token in tokens)
    if forbidden:
        raise SQLExecutionValidationError(
            "query contains forbidden token(s): " + ", ".join(forbidden)
        )


def _strip_sql_comments_and_strings(sql: str) -> str:
    result: list[str] = []
    index = 0
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False
    while index < len(sql):
        current = sql[index]
        nxt = sql[index + 1] if index + 1 < len(sql) else ""

        if in_line_comment:
            if current == "\n":
                in_line_comment = False
                result.append("\n")
            else:
                result.append(" ")
            index += 1
            continue
        if in_block_comment:
            if current == "*" and nxt == "/":
                in_block_comment = False
                result.extend((" ", " "))
                index += 2
            else:
                result.append(" ")
                index += 1
            continue
        if in_single:
            if current == "'" and nxt == "'":
                result.extend((" ", " "))
                index += 2
                continue
            if current == "'":
                in_single = False
            result.append(" ")
            index += 1
            continue
        if in_double:
            if current == '"' and nxt == '"':
                result.extend((" ", " "))
                index += 2
                continue
            if current == '"':
                in_double = False
            result.append(" ")
            index += 1
            continue

        if current == "-" and nxt == "-":
            in_line_comment = True
            result.extend((" ", " "))
            index += 2
            continue
        if current == "/" and nxt == "*":
            in_block_comment = True
            result.extend((" ", " "))
            index += 2
            continue
        if current == "'":
            in_single = True
            result.append(" ")
            index += 1
            continue
        if current == '"':
            in_double = True
            result.append(" ")
            index += 1
            continue
        result.append(current)
        index += 1
    return "".join(result)


def _normalize_params(params: SqlParams | None) -> SqlParams | tuple[()]:
    if params is None:
        return ()
    if isinstance(params, Mapping):
        collisions = sorted(_RESERVED_PAGING_PARAM_KEYS.intersection(params))
        if collisions:
            raise SQLExecutionValidationError(
                "params cannot define reserved paging key(s): " + ", ".join(collisions)
            )
        return params
    if isinstance(params, (str, bytes, bytearray)):
        raise SQLExecutionValidationError("params must be a mapping or positional list/tuple")
    if isinstance(params, tuple):
        return params
    if isinstance(params, list):
        return tuple(params)
    raise SQLExecutionValidationError("params must be a mapping or positional list/tuple")


def _count_params(params: SqlParams | tuple[()]) -> SqlParams | tuple[()]:
    return params


def _paged_params(params: SqlParams | tuple[()], *, limit: int, offset: int) -> SqlParams:
    if params == ():
        return (limit, offset)
    if isinstance(params, Mapping):
        return {
            **params,
            "_pd_limit": limit,
            "_pd_offset": offset,
        }
    return (*params, limit, offset)


def _count_query(sql: str) -> str:
    return f"SELECT COUNT(*) AS _pd_total_rows FROM ({sql}) AS _pd_count"


def _paged_query(sql: str, *, params: SqlParams | None, dialect: DatabaseDialect) -> str:
    if dialect == "postgresql":
        if isinstance(params, Mapping):
            return f"SELECT * FROM ({sql}) AS _pd_page LIMIT %(_pd_limit)s OFFSET %(_pd_offset)s"
        return f"SELECT * FROM ({sql}) AS _pd_page LIMIT %s OFFSET %s"
    if isinstance(params, Mapping):
        return f"SELECT * FROM ({sql}) AS _pd_page LIMIT :_pd_limit OFFSET :_pd_offset"
    return f"SELECT * FROM ({sql}) AS _pd_page LIMIT ? OFFSET ?"


def _error_code(exc: Exception) -> str:
    if isinstance(exc, SQLRowLimitExceededError):
        return "ROW_LIMIT_EXCEEDED"
    if isinstance(exc, SQLExecutionValidationError):
        return "INVALID_REQUEST"
    message = str(exc).lower()
    if isinstance(exc, TimeoutError) or _is_timeout_message(message):
        return "TIMEOUT"
    if "syntax error" in message:
        return "SYNTAX_ERROR"
    return "EXECUTION_ERROR"


def _is_timeout_message(message: str) -> bool:
    lowered = message.lower()
    return (
        "statement timeout" in lowered
        or "timed out" in lowered
        or "interrupted" in lowered
        or "canceling statement due to statement timeout" in lowered
    )


def _is_timeout_exception(exc: Exception) -> bool:
    return isinstance(exc, TimeoutError) or _is_timeout_message(str(exc))


def _set_timeout_handler(
    connection: DBConnection,
    *,
    dialect: DatabaseDialect,
    timeout_ms: int,
    deadline_s: float,
    clock: Callable[[], float],
) -> None:
    raw_setter = getattr(connection, "set_progress_handler", None)
    if raw_setter is None or not callable(raw_setter):
        if dialect == "postgresql":
            connection.execute(f"SET statement_timeout = {int(timeout_ms)}")
        return

    def _check_timeout() -> int:
        return 1 if clock() > deadline_s else 0

    setter = cast(Callable[[Callable[[], int] | None, int], None], raw_setter)
    setter(_check_timeout, 1_000)


def _clear_timeout_handler(connection: DBConnection, *, dialect: DatabaseDialect) -> None:
    raw_setter = getattr(connection, "set_progress_handler", None)
    if raw_setter is None or not callable(raw_setter):
        if dialect == "postgresql":
            try:
                connection.execute("SET statement_timeout = DEFAULT")
            except Exception:  # noqa: BLE001
                # Timeout/cancel errors can leave Postgres in aborted transaction state.
                rollback = getattr(connection, "rollback", None)
                if callable(rollback):
                    rollback()
                    connection.execute("SET statement_timeout = DEFAULT")
        return
    setter = cast(Callable[[Callable[[], int] | None, int], None], raw_setter)
    setter(None, 0)


def execute_sql_query(
    *,
    connection: DBConnection,
    request: SQLQueryRequest,
    caller_key_id: str,
    dialect: DatabaseDialect = "sqlite",
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
        params = _normalize_params(request.params)

        deadline = start + (request.timeout_ms / 1000.0)
        _set_timeout_handler(
            connection,
            dialect=dialect,
            timeout_ms=request.timeout_ms,
            deadline_s=deadline,
            clock=clock,
        )

        count_cursor = connection.execute(_count_query(sql), _count_params(params))
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
            _paged_query(sql, params=params, dialect=dialect),
            _paged_params(params, limit=page_limit, offset=offset),
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
        if _is_timeout_exception(exc):
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
        _clear_timeout_handler(connection, dialect=dialect)
