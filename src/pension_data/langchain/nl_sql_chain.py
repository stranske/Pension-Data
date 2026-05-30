"""LangChain-style NL-to-SQL execution with LangSmith-style trace hooks."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Literal, Protocol
from uuid import uuid4

from pension_data.langchain.tracing import langsmith_tracing_context
from pension_data.query.sql_safety import (
    AmbiguousPromptError,
    SQLSafetyPolicy,
    SQLSafetyValidationError,
    default_nl_query_policy,
    extract_selected_columns,
    validate_nl_prompt,
    validate_result_columns,
    validate_sql_policy,
)

SqlParams = Mapping[str, Any] | tuple[Any, ...] | list[Any]
NLToSQLStatus = Literal["ok", "error"]
NLToSQLPolicy = SQLSafetyPolicy

NL_TO_SQL_TRACE_ENTRYPOINTS: tuple[str, ...] = (
    "pension_data.api.routes.nl.run_nl_query_endpoint",
    "pension_data.langchain.nl_sql_chain.run_nl_sql_chain",
)
NL_TO_SQL_TRACE_STAGES_SUCCESS: tuple[str, ...] = (
    "nl.prompt.received",
    "nl.sql.generated",
    "nl.sql.validated",
    "nl.sql.executed",
)
NL_TO_SQL_TRACE_STAGE_ERROR: str = "nl.sql.error"
NL_TO_SQL_TRACE_ERROR_STAGES: tuple[str, ...] = ("request", "validation", "execution")


def nl_to_sql_trace_stages(
    *,
    status: NLToSQLStatus,
    error_stage: Literal["request", "validation", "execution"] = "execution",
) -> tuple[str, ...]:
    """Return ordered lifecycle stages that should be traced for a run status.

    The ``error_stage`` argument names the point in the lifecycle where the
    error originated and therefore which prior stages were already emitted:

    - ``request``: pre-SQL failure (request bounds, ambiguous prompt). Only
      ``nl.prompt.received`` was emitted before ``nl.sql.error``.
    - ``validation``: SQL was generated but rejected by the read-only safety
      policy. ``nl.prompt.received`` and ``nl.sql.generated`` were emitted.
    - ``execution``: SQL was generated and validated but failed during query
      execution. All three pre-execution stages were emitted.
    """

    if status == "ok":
        return NL_TO_SQL_TRACE_STAGES_SUCCESS
    if error_stage == "request":
        return (NL_TO_SQL_TRACE_STAGES_SUCCESS[0], NL_TO_SQL_TRACE_STAGE_ERROR)
    if error_stage == "validation":
        return (*NL_TO_SQL_TRACE_STAGES_SUCCESS[:2], NL_TO_SQL_TRACE_STAGE_ERROR)
    return (*NL_TO_SQL_TRACE_STAGES_SUCCESS[:3], NL_TO_SQL_TRACE_STAGE_ERROR)


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
    cost: Mapping[str, Any] | None = None


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


class NLRequestValidationError(ValueError):
    """Raised when request inputs violate deterministic NL policy bounds."""


def _normalize_params(params: SqlParams | None) -> SqlParams | tuple[()]:
    if params is None:
        return ()
    if isinstance(params, Mapping):
        return params
    if isinstance(params, (str, bytes, bytearray)):
        raise NLRequestValidationError("params must be a mapping or positional list/tuple")
    if isinstance(params, tuple):
        return params
    if isinstance(params, list):
        return tuple(params)
    raise NLRequestValidationError("params must be a mapping or positional list/tuple")


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


def _extract_cost(generated: str | Mapping[str, Any]) -> Mapping[str, Any] | None:
    if isinstance(generated, str):
        return None
    usage_sources: list[Mapping[str, Any]] = []
    direct_usage = generated.get("usage")
    if isinstance(direct_usage, Mapping):
        usage_sources.append(direct_usage)
    direct_token_usage = generated.get("token_usage")
    if isinstance(direct_token_usage, Mapping):
        usage_sources.append(direct_token_usage)
    response_metadata = generated.get("response_metadata")
    if isinstance(response_metadata, Mapping):
        token_usage = response_metadata.get("token_usage")
        if isinstance(token_usage, Mapping):
            usage_sources.append(token_usage)
        usage_metadata = response_metadata.get("usage_metadata")
        if isinstance(usage_metadata, Mapping):
            usage_sources.append(usage_metadata)
    usage_metadata = generated.get("usage_metadata")
    if isinstance(usage_metadata, Mapping):
        usage_sources.append(usage_metadata)

    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    cost_usd: float | None = None
    for usage in usage_sources:
        if prompt_tokens is None:
            prompt_tokens = _safe_int(
                usage.get("prompt_tokens")
                or usage.get("input_tokens")
                or usage.get("input_token_count")
                or usage.get("prompt_token_count")
            )
        if completion_tokens is None:
            completion_tokens = _safe_int(
                usage.get("completion_tokens")
                or usage.get("output_tokens")
                or usage.get("output_token_count")
                or usage.get("completion_token_count")
            )
        if total_tokens is None:
            total_tokens = _safe_int(usage.get("total_tokens") or usage.get("total_token_count"))
        if cost_usd is None:
            cost_usd = _safe_float(usage.get("cost_usd"))
    if total_tokens is None and (prompt_tokens is not None or completion_tokens is not None):
        total_tokens = (prompt_tokens or 0) + (completion_tokens or 0)
    if (
        prompt_tokens is None
        and completion_tokens is None
        and total_tokens is None
        and cost_usd is None
    ):
        return None
    return {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "cost_usd": cost_usd,
    }


def _safe_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        token = value.strip()
        if token.isdigit():
            return int(token)
    return None


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
    if isinstance(exc, NLRequestValidationError):
        return "INVALID_REQUEST"
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
        cost: Mapping[str, Any] | None = None,
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
                cost=cost,
            ),
            error=error,
        )

    with langsmith_tracing_context(
        name="nl_to_sql.query",
        run_type="chain",
        inputs={"request_id": request_id, "question": request.question},
        metadata={
            "surface": "nl-to-sql",
            "entrypoint": "pension_data.langchain.nl_sql_chain.run_nl_sql_chain",
            "max_rows": request.max_rows,
            "timeout_ms": request.timeout_ms,
        },
    ):
        sql: str | None = None
        cost: Mapping[str, Any] | None = None
        active_policy = policy or default_nl_query_policy()
        _emit_trace(
            trace_sink,
            emitted_events,
            stage="nl.prompt.received",
            payload={"request_id": request_id, "question": request.question},
        )
        try:
            if request.max_rows < 1:
                raise NLRequestValidationError("max_rows must be >= 1")
            if request.timeout_ms < 1:
                raise NLRequestValidationError("timeout_ms must be >= 1")
            if request.max_rows > active_policy.max_rows:
                raise NLRequestValidationError(
                    f"max_rows must be <= policy max_rows ({active_policy.max_rows})"
                )
            if request.timeout_ms > active_policy.max_timeout_ms:
                raise NLRequestValidationError(
                    "timeout_ms exceeds policy max_timeout_ms " f"({active_policy.max_timeout_ms})"
                )
            params = _normalize_params(request.params)
            question = validate_nl_prompt(request.question)

            generated = chain.invoke({"question": question, "dialect": "sqlite"})
            cost = _extract_cost(generated)
            sql = _extract_sql(generated)
            _emit_trace(
                trace_sink,
                emitted_events,
                stage="nl.sql.generated",
                payload={"request_id": request_id, "sql": sql},
            )
            sql = validate_sql_policy(sql, policy=active_policy)
            if active_policy.require_source_document_id:
                selected_columns = extract_selected_columns(sql)
                if "source_document_id" not in set(selected_columns):
                    raise SQLSafetyValidationError(
                        "generated SQL must include source_document_id in SELECT for provenance metadata"
                    )
            _emit_trace(
                trace_sink,
                emitted_events,
                stage="nl.sql.validated",
                payload={
                    "request_id": request_id,
                    "status": "ok",
                    "sql_validation_status": "pass",
                    "read_only_status": "read_only",
                },
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
                cost=cost,
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
                cost=cost,
            )
        finally:
            _clear_timeout_handler(connection)
