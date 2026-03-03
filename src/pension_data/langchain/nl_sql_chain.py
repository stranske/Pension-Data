"""LangChain-style NL-to-SQL execution with LangSmith-style trace hooks."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Literal, Protocol
from uuid import uuid4

from pension_data.query.sql_safety import (
    AmbiguousPromptError,
    SQLSafetyPolicy,
    SQLSafetyValidationError,
    default_nl_query_policy,
    validate_nl_prompt,
    validate_result_columns,
    validate_sql_policy,
)

SqlParams = Mapping[str, Any] | tuple[Any, ...] | list[Any]
NLToSQLStatus = Literal["ok", "error"]
NLToSQLPolicy = SQLSafetyPolicy


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
class NLToSQLProvenanceRow:
    """Provenance metadata for one returned SQL row."""

    row_index: int
    source_document_id: str | None
    evidence_refs: tuple[str, ...]
    confidence: float | None


@dataclass(frozen=True, slots=True)
class NLToSQLResponse:
    """Deterministic NL-to-SQL execution response envelope."""

    status: NLToSQLStatus
    sql: str | None
    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]
    provenance: tuple[NLToSQLProvenanceRow, ...]
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


def _safe_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        token = value.strip()
        if not token:
            return None
        try:
            return float(token)
        except ValueError:
            return None
    return None


def _parse_evidence_refs(raw: Any) -> tuple[str, ...]:
    if raw is None:
        return ()
    if isinstance(raw, str):
        token = raw.strip()
        if not token:
            return ()
        if token.startswith("[") and token.endswith("]"):
            try:
                parsed = json.loads(token)
            except ValueError:
                parsed = None
            if isinstance(parsed, list):
                refs = [item.strip() for item in parsed if isinstance(item, str) and item.strip()]
                return tuple(dict.fromkeys(refs))
        refs = [value.strip() for value in token.split(",") if value.strip()]
        return tuple(dict.fromkeys(refs))
    if isinstance(raw, (tuple, list)):
        refs = [item.strip() for item in raw if isinstance(item, str) and item.strip()]
        return tuple(dict.fromkeys(refs))
    return ()


def _build_provenance(
    *, columns: tuple[str, ...], rows: tuple[tuple[Any, ...], ...]
) -> tuple[NLToSQLProvenanceRow, ...]:
    column_index = {name.lower(): idx for idx, name in enumerate(columns)}
    source_idx = column_index.get("source_document_id")
    evidence_idx = column_index.get("evidence_refs")
    confidence_idx = column_index.get("confidence")

    provenance_rows: list[NLToSQLProvenanceRow] = []
    for row_index, row in enumerate(rows):
        source_document_id: str | None = None
        if source_idx is not None and source_idx < len(row):
            raw_source = row[source_idx]
            if isinstance(raw_source, str) and raw_source.strip():
                source_document_id = raw_source.strip()
        evidence_refs: tuple[str, ...] = ()
        if evidence_idx is not None and evidence_idx < len(row):
            evidence_refs = _parse_evidence_refs(row[evidence_idx])
        confidence: float | None = None
        if confidence_idx is not None and confidence_idx < len(row):
            confidence = _safe_float(row[confidence_idx])
        provenance_rows.append(
            NLToSQLProvenanceRow(
                row_index=row_index,
                source_document_id=source_document_id,
                evidence_refs=evidence_refs,
                confidence=confidence,
            )
        )
    return tuple(provenance_rows)


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
    policy: SQLSafetyPolicy | None = None,
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
        provenance: tuple[NLToSQLProvenanceRow, ...],
        error: NLToSQLError | None,
    ) -> NLToSQLResponse:
        duration_ms = max(0, int(round((perf_counter() - started) * 1000)))
        return NLToSQLResponse(
            status=status,
            sql=sql,
            columns=columns,
            rows=rows,
            provenance=provenance,
            metadata=NLToSQLMetadata(
                request_id=request_id,
                duration_ms=duration_ms,
                returned_rows=len(rows),
                trace_event_count=len(emitted_events),
            ),
            error=error,
        )

    sql: str | None = None
    active_policy = policy or default_nl_query_policy()
    try:
        if request.max_rows < 1:
            raise ValueError("max_rows must be >= 1")
        if request.timeout_ms < 1:
            raise ValueError("timeout_ms must be >= 1")
        if request.max_rows > active_policy.max_rows:
            raise ValueError(f"max_rows must be <= policy max_rows ({active_policy.max_rows})")
        if request.timeout_ms > active_policy.max_timeout_ms:
            raise ValueError(
                "timeout_ms exceeds policy max_timeout_ms " f"({active_policy.max_timeout_ms})"
            )
        params = _normalize_params(request.params)
        question = validate_nl_prompt(request.question)
        _emit_trace(
            trace_sink,
            emitted_events,
            stage="nl.prompt.received",
            payload={"request_id": request_id, "question": question},
        )

        generated = chain.invoke({"question": question, "dialect": "sqlite"})
        sql = validate_sql_policy(_extract_sql(generated), policy=active_policy)
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
        validate_result_columns(columns, policy=active_policy)
        fetched = tuple(tuple(row) for row in cursor.fetchmany(request.max_rows + 1))
        if len(fetched) > request.max_rows:
            raise MaxRowsExceededError(
                f"generated SQL exceeded max_rows limit ({request.max_rows})"
            )
        provenance = _build_provenance(columns=columns, rows=fetched)
        if "source_document_id" in {column.lower() for column in columns} and any(
            row.source_document_id is None for row in provenance
        ):
            raise SQLSafetyValidationError(
                "source_document_id must be populated for provenance-tracked rows"
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
            provenance=provenance,
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
            provenance=(),
            error=NLToSQLError(code=_error_code(exc), message=str(exc)),
        )
    finally:
        _clear_timeout_handler(connection)
