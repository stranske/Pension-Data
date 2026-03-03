"""LangChain-style NL-to-SQL execution with LangSmith-style trace hooks."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Literal, Protocol
from uuid import uuid4

from pension_data.query.sql_safety import (
    AmbiguousPromptError,
    SQLSafetyValidationError,
    validate_nl_prompt,
    validate_read_only_sql,
)

SqlParams = Mapping[str, Any] | tuple[Any, ...] | list[Any]
NLToSQLStatus = Literal["ok", "error"]


@dataclass(frozen=True, slots=True)
class NLToSQLRequest:
    """NL-to-SQL request contract with execution guardrails."""

    question: str
    params: SqlParams | None = None
    max_rows: int = 500
    timeout_ms: int = 2_000


@dataclass(frozen=True, slots=True)
class NLToSQLError:
    """Deterministic NL-to-SQL error payload."""

    code: str
    message: str


@dataclass(frozen=True, slots=True)
class NLToSQLMetadata:
    """NL-to-SQL metadata and timing information."""

    request_id: str
    duration_ms: int
    returned_rows: int
    trace_event_count: int


@dataclass(frozen=True, slots=True)
class NLToSQLResponse:
    """Deterministic NL-to-SQL execution response envelope."""

    status: NLToSQLStatus
    sql: str | None
    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]
    metadata: NLToSQLMetadata
    error: NLToSQLError | None


@dataclass(frozen=True, slots=True)
class LangSmithTraceEvent:
    """One LangSmith-style trace event emitted by the NL query lifecycle."""

    stage: str
    payload: dict[str, Any]


class NLToSQLChain(Protocol):
    """Minimal LangChain-style interface for NL question to SQL generation."""

    def invoke(self, values: Mapping[str, Any]) -> str | Mapping[str, Any]:
        """Generate SQL from NL inputs."""


class LangSmithTraceSink(Protocol):
    """Sink interface for emitting LangSmith-style trace events."""

    def emit(self, event: LangSmithTraceEvent) -> None:
        """Record one trace event."""


@dataclass(slots=True)
class InMemoryLangSmithTraceSink:
    """Simple in-memory trace collector for tests and local observability."""

    events: list[LangSmithTraceEvent]

    def emit(self, event: LangSmithTraceEvent) -> None:
        self.events.append(event)


class MaxRowsExceededError(ValueError):
    """Raised when generated SQL returns more rows than allowed by request.max_rows."""


def _normalize_params(params: SqlParams | None) -> SqlParams | tuple[()]:
    if params is None:
        return ()
    if isinstance(params, Mapping):
        return params
    if isinstance(params, (str, bytes, bytearray)):
        raise ValueError("params must be a mapping or positional list/tuple")
    if isinstance(params, tuple):
        return params
    if isinstance(params, list):
        return tuple(params)
    raise ValueError("params must be a mapping or positional list/tuple")


def _set_timeout_handler(connection: sqlite3.Connection, *, deadline_s: float) -> None:
    def _check_timeout() -> int:
        return 1 if perf_counter() > deadline_s else 0

    connection.set_progress_handler(_check_timeout, 1_000)


def _clear_timeout_handler(connection: sqlite3.Connection) -> None:
    connection.set_progress_handler(None, 0)


def _emit_trace(
    trace_sink: LangSmithTraceSink | None,
    events: list[LangSmithTraceEvent],
    *,
    stage: str,
    payload: Mapping[str, Any],
) -> None:
    event = LangSmithTraceEvent(stage=stage, payload=dict(payload))
    events.append(event)
    if trace_sink is not None:
        trace_sink.emit(event)


def _extract_sql(generated: str | Mapping[str, Any]) -> str:
    if isinstance(generated, str):
        return generated
    sql = generated.get("sql")
    if not isinstance(sql, str):
        raise ValueError("chain output must include a string `sql` field")
    return sql


def _error_code(exc: Exception) -> str:
    if isinstance(exc, AmbiguousPromptError):
        return "AMBIGUOUS_PROMPT"
    if isinstance(exc, SQLSafetyValidationError):
        return "UNSAFE_SQL"
    if isinstance(exc, MaxRowsExceededError):
        return "MAX_ROWS_EXCEEDED"
    if isinstance(exc, TimeoutError):
        return "TIMEOUT"
    if isinstance(exc, sqlite3.OperationalError):
        if "syntax error" in str(exc).lower():
            return "SYNTAX_ERROR"
        if "interrupted" in str(exc).lower():
            return "TIMEOUT"
    return "EXECUTION_ERROR"


def run_nl_sql_chain(
    *,
    connection: sqlite3.Connection,
    request: NLToSQLRequest,
    chain: NLToSQLChain,
    trace_sink: LangSmithTraceSink | None = None,
) -> NLToSQLResponse:
    """Generate SQL from NL prompt, enforce read-only policy, and execute query."""
    request_id = f"nlq:{uuid4().hex}"
    started = perf_counter()
    emitted_events: list[LangSmithTraceEvent] = []

    def _finalize(
        *,
        status: NLToSQLStatus,
        sql: str | None,
        columns: tuple[str, ...],
        rows: tuple[tuple[Any, ...], ...],
        error: NLToSQLError | None,
    ) -> NLToSQLResponse:
        duration_ms = max(0, int(round((perf_counter() - started) * 1000)))
        return NLToSQLResponse(
            status=status,
            sql=sql,
            columns=columns,
            rows=rows,
            metadata=NLToSQLMetadata(
                request_id=request_id,
                duration_ms=duration_ms,
                returned_rows=len(rows),
                trace_event_count=len(emitted_events),
            ),
            error=error,
        )

    sql: str | None = None
    try:
        if request.max_rows < 1:
            raise ValueError("max_rows must be >= 1")
        if request.timeout_ms < 1:
            raise ValueError("timeout_ms must be >= 1")
        params = _normalize_params(request.params)
        question = validate_nl_prompt(request.question)
        _emit_trace(
            trace_sink,
            emitted_events,
            stage="nl.prompt.received",
            payload={"request_id": request_id, "question": question},
        )

        generated = chain.invoke({"question": question, "dialect": "sqlite"})
        sql = validate_read_only_sql(_extract_sql(generated))
        _emit_trace(
            trace_sink,
            emitted_events,
            stage="nl.sql.generated",
            payload={"request_id": request_id, "sql": sql},
        )

        deadline = perf_counter() + (request.timeout_ms / 1000.0)
        _set_timeout_handler(connection, deadline_s=deadline)
        cursor = connection.execute(sql, params)
        columns = tuple(column[0] for column in (cursor.description or ()))
        fetched = tuple(tuple(row) for row in cursor.fetchmany(request.max_rows + 1))
        if len(fetched) > request.max_rows:
            raise MaxRowsExceededError(
                f"generated SQL exceeded max_rows limit ({request.max_rows})"
            )
        _emit_trace(
            trace_sink,
            emitted_events,
            stage="nl.sql.executed",
            payload={
                "request_id": request_id,
                "status": "ok",
                "row_count": len(fetched),
            },
        )
        return _finalize(
            status="ok",
            sql=sql,
            columns=columns,
            rows=fetched,
            error=None,
        )
    except Exception as exc:  # noqa: BLE001
        if isinstance(exc, sqlite3.OperationalError) and "interrupted" in str(exc).lower():
            exc = TimeoutError("query timed out before completion")
        _emit_trace(
            trace_sink,
            emitted_events,
            stage="nl.sql.error",
            payload={
                "request_id": request_id,
                "status": "error",
                "error_code": _error_code(exc),
                "message": str(exc),
            },
        )
        return _finalize(
            status="error",
            sql=sql,
            columns=(),
            rows=(),
            error=NLToSQLError(code=_error_code(exc), message=str(exc)),
        )
    finally:
        _clear_timeout_handler(connection)
