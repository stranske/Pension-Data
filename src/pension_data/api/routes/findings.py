"""Route adapters for findings explain/compare workflows."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from pension_data.api.auth import SCOPE_NL, APIKeyStore, authenticate_request, build_audit_event
from pension_data.langchain.findings_compare import (
    CompareRequest,
    CompareResponse,
    FindingsCompareChain,
    run_findings_compare_chain,
)
from pension_data.langchain.findings_explain import (
    ExplainRequest,
    ExplainResponse,
    FindingsExplainChain,
    run_findings_explain_chain,
)


@dataclass(frozen=True, slots=True)
class FindingsExplainRouteResult:
    """Explain endpoint result bundle with audit metadata."""

    response: ExplainResponse
    audit_event: dict[str, Any]


@dataclass(frozen=True, slots=True)
class FindingsCompareRouteResult:
    """Compare endpoint result bundle with audit metadata."""

    response: CompareResponse
    audit_event: dict[str, Any]


def run_findings_explain_endpoint(
    *,
    api_key_header: str | None,
    key_store: APIKeyStore,
    request: ExplainRequest,
    chain: FindingsExplainChain,
    event: Mapping[str, Any] | None = None,
) -> FindingsExplainRouteResult:
    """Authenticate and execute a findings explain request."""
    auth_context = authenticate_request(
        api_key_header=api_key_header,
        required_scope=SCOPE_NL,
        key_store=key_store,
    )
    response = run_findings_explain_chain(request=request, chain=chain)
    event_payload = dict(event or {})
    event_payload.update(
        {
            "request_id": response.metadata.request_id,
            "query_status": response.status,
            "citation_count": len(response.result.citations) if response.result is not None else 0,
        }
    )
    return FindingsExplainRouteResult(
        response=response,
        audit_event=build_audit_event(
            operation="nl.findings.explain",
            auth_context=auth_context,
            event=event_payload,
        ),
    )


def run_findings_compare_endpoint(
    *,
    api_key_header: str | None,
    key_store: APIKeyStore,
    request: CompareRequest,
    chain: FindingsCompareChain,
    event: Mapping[str, Any] | None = None,
) -> FindingsCompareRouteResult:
    """Authenticate and execute a findings compare request."""
    auth_context = authenticate_request(
        api_key_header=api_key_header,
        required_scope=SCOPE_NL,
        key_store=key_store,
    )
    response = run_findings_compare_chain(request=request, chain=chain)
    event_payload = dict(event or {})
    event_payload.update(
        {
            "request_id": response.metadata.request_id,
            "query_status": response.status,
            "citation_count": len(response.result.citations) if response.result is not None else 0,
        }
    )
    return FindingsCompareRouteResult(
        response=response,
        audit_event=build_audit_event(
            operation="nl.findings.compare",
            auth_context=auth_context,
            event=event_payload,
        ),
    )
