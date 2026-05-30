"""Route-layer adapter for audited SQL query execution."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pension_data.api.auth import SCOPE_QUERY, APIKeyStore, authenticate_request, build_audit_event
from pension_data.query.run_record import (
    QueryRunActor,
    QueryRunArtifact,
    QueryRunRecord,
    default_run_record_root,
    record_relative_path,
    write_query_run_record,
)
from pension_data.query.sql_service import (
    SQLExecutionAuditLog,
    SQLQueryRequest,
    SQLQueryResponse,
    execute_sql_query,
)


@dataclass(frozen=True, slots=True)
class SQLRouteResult:
    """Route response bundle containing SQL envelope and audit event payload."""

    response: SQLQueryResponse
    audit_event: dict[str, Any]


def run_sql_query_endpoint(
    *,
    api_key_header: str | None,
    key_store: APIKeyStore,
    connection: sqlite3.Connection,
    request: SQLQueryRequest,
    audit_log_store: list[SQLExecutionAuditLog] | None = None,
    event: Mapping[str, Any] | None = None,
    run_record_root: Path | None = None,
) -> SQLRouteResult:
    """Execute one authenticated SQL query and return envelope + audit event."""
    auth_context = authenticate_request(
        api_key_header=api_key_header,
        required_scope=SCOPE_QUERY,
        key_store=key_store,
    )
    response = execute_sql_query(
        connection=connection,
        request=request,
        caller_key_id=auth_context.key_id,
        audit_log_store=audit_log_store,
    )
    with suppress(Exception):
        _persist_sql_query_run_record(
            request=request,
            response=response,
            key_id=auth_context.key_id,
            scopes=auth_context.scopes,
            root=run_record_root,
        )
    event_payload = dict(event or {})
    event_payload.update(
        {
            "query_id": response.metadata.query_id,
            "query_status": response.status,
            "query_row_count": response.metadata.returned_rows,
            "query_error_code": response.error.code if response.error is not None else None,
        }
    )
    return SQLRouteResult(
        response=response,
        audit_event=build_audit_event(
            operation="query.run",
            auth_context=auth_context,
            event=event_payload,
        ),
    )


def _persist_sql_query_run_record(
    *,
    request: SQLQueryRequest,
    response: SQLQueryResponse,
    key_id: str,
    scopes: tuple[str, ...],
    root: Path | None,
) -> None:
    artifact_root = root or default_run_record_root()
    safe_run_id = "".join(
        char if char.isalnum() or char in {"-", "_"} else "-" for char in response.metadata.query_id
    )
    rows_path = artifact_root / "query" / "sql_runs" / "rows" / f"{safe_run_id}.json"
    rows_artifact = QueryRunArtifact(
        name="sql-query-rows",
        path=record_relative_path(rows_path, root=artifact_root),
        content_type="application/json",
        row_count=response.metadata.returned_rows,
    )
    record = QueryRunRecord(
        run_id=response.metadata.query_id,
        surface="sql",
        status=response.status,
        who=QueryRunActor(
            key_id=key_id,
            scopes=scopes,
            required_scope=SCOPE_QUERY,
            correlation_id=None,
        ),
        inputs={
            "sql": request.sql,
            "params": request.params,
            "page": request.page,
            "page_size": request.page_size,
            "timeout_ms": request.timeout_ms,
            "max_rows": request.max_rows,
        },
        generated_sql=None,
        executed_sql=request.sql,
        columns=response.columns,
        row_count=response.metadata.returned_rows,
        rows_artifact=rows_artifact,
        provenance=(),
        warnings=(),
        error=(
            None
            if response.error is None
            else {"code": response.error.code, "message": response.error.message}
        ),
        duration_ms=response.metadata.duration_ms,
        cost=None,
        artifacts=(rows_artifact,),
    )
    write_query_run_record(
        root=artifact_root,
        surface="sql",
        run_id=response.metadata.query_id,
        record=record,
        rows=response.rows,
    )
