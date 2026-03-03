"""Route-layer adapter for audited SQL query execution."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from pension_data.api.auth import SCOPE_QUERY, APIKeyStore, authenticate_request, build_audit_event
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
