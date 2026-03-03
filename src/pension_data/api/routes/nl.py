"""Route adapter for authenticated NL-to-SQL execution."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pension_data.api.auth import SCOPE_NL, APIKeyStore, authenticate_request, build_audit_event
from pension_data.langchain.nl_sql_chain import (
    LangSmithTraceSink,
    NLToSQLChain,
    NLToSQLPolicy,
    NLToSQLRequest,
    NLToSQLResponse,
    run_nl_sql_chain,
)
from pension_data.langchain.observability import (
    append_nl_operation_log,
    build_nl_operation_log_entry,
    default_nl_log_path,
)


@dataclass(frozen=True, slots=True)
class NLRouteResult:
    """Route-layer NL query result bundle with audit event payload."""

    response: NLToSQLResponse
    audit_event: dict[str, Any]


def run_nl_query_endpoint(
    *,
    api_key_header: str | None,
    key_store: APIKeyStore,
    connection: sqlite3.Connection,
    request: NLToSQLRequest,
    chain: NLToSQLChain,
    trace_sink: LangSmithTraceSink | None = None,
    policy: NLToSQLPolicy | None = None,
    provider: str = "unknown",
    model: str = "unknown",
    correlation_id: str | None = None,
    log_path: Path | None = None,
    log_retention_limit: int = 2_000,
    event: Mapping[str, Any] | None = None,
) -> NLRouteResult:
    """Authenticate and execute one NL-to-SQL request with audit metadata."""
    auth_context = authenticate_request(
        api_key_header=api_key_header,
        required_scope=SCOPE_NL,
        key_store=key_store,
    )
    response = run_nl_sql_chain(
        connection=connection,
        request=request,
        chain=chain,
        trace_sink=trace_sink,
        policy=policy,
    )
    entry = build_nl_operation_log_entry(
        request=request,
        response=response,
        provider=provider,
        model=model,
        correlation_id=correlation_id,
    )
    append_nl_operation_log(
        path=log_path or default_nl_log_path(),
        entry=entry,
        retention_limit=log_retention_limit,
    )
    event_payload = dict(event or {})
    event_payload.update(
        {
            "request_id": response.metadata.request_id,
            "correlation_id": entry.correlation_id,
            "query_status": response.status,
            "generated_sql": response.sql,
            "returned_rows": response.metadata.returned_rows,
            "error_code": response.error.code if response.error is not None else None,
            "provider": entry.provider,
            "model": entry.model,
        }
    )
    return NLRouteResult(
        response=response,
        audit_event=build_audit_event(
            operation="nl.ask",
            auth_context=auth_context,
            event=event_payload,
        ),
    )
