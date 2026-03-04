"""Route adapter for authenticated NL-to-SQL execution."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
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
    event_payload = dict(event or {})
    event_payload.update(
        {
            "request_id": response.metadata.request_id,
            "query_status": response.status,
            "generated_sql": response.sql,
            "returned_rows": response.metadata.returned_rows,
            "error_code": response.error.code if response.error is not None else None,
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
