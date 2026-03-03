"""Route adapter for metric-history query execution."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from pension_data.api.auth import SCOPE_QUERY, APIKeyStore, authenticate_request, build_audit_event
from pension_data.query.metric_history_service import (
    MetricHistoryRequest,
    MetricHistoryResponse,
    MetricHistoryRow,
    query_metric_history,
)


@dataclass(frozen=True, slots=True)
class MetricHistoryRouteResult:
    """Route response bundle containing metric-history response and audit payload."""

    response: MetricHistoryResponse
    audit_event: dict[str, Any]


def run_metric_history_endpoint(
    *,
    api_key_header: str | None,
    key_store: APIKeyStore,
    request: MetricHistoryRequest,
    rows: Sequence[MetricHistoryRow],
    event: Mapping[str, Any] | None = None,
) -> MetricHistoryRouteResult:
    """Execute one authenticated metric-history query with audit context."""
    auth_context = authenticate_request(
        api_key_header=api_key_header,
        required_scope=SCOPE_QUERY,
        key_store=key_store,
    )
    response = query_metric_history(rows, request=request)

    event_payload = dict(event or {})
    event_payload.update(
        {
            "entity_id": request.entity_id.strip(),
            "metric_name": request.metric_name,
            "metric_family": request.metric_family,
            "returned_rows": len(response.rows),
            "total_rows": response.total_rows,
        }
    )
    return MetricHistoryRouteResult(
        response=response,
        audit_event=build_audit_event(
            operation="query.metric_history",
            auth_context=auth_context,
            event=event_payload,
        ),
    )
