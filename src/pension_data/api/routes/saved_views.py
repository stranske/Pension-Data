"""Route-layer adapter for audited saved analytical view execution."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from pension_data.api.auth import SCOPE_QUERY, APIKeyStore, authenticate_request, build_audit_event
from pension_data.query.saved_views.models import (
    AllocationPeerInput,
    AllocationPeerRow,
    FundingTrendInput,
    FundingTrendRow,
    HoldingsOverlapInput,
    HoldingsOverlapRow,
)
from pension_data.query.saved_views.service import (
    execute_allocation_peer_compare_view,
    execute_funding_trend_view,
    execute_holdings_overlap_view,
)


@dataclass(frozen=True, slots=True)
class SavedViewRouteResult:
    """Route response bundle for saved analytical views."""

    view_name: str
    rows: list[FundingTrendRow] | list[AllocationPeerRow] | list[HoldingsOverlapRow]
    audit_event: dict[str, Any]


def run_saved_view_endpoint(
    *,
    api_key_header: str | None,
    key_store: APIKeyStore,
    view_name: str,
    view_inputs: list[FundingTrendInput] | list[AllocationPeerInput] | list[HoldingsOverlapInput],
    subject_plan_id: str | None = None,
    plan_period: str | None = None,
    event: Mapping[str, Any] | None = None,
) -> SavedViewRouteResult:
    """Execute one authenticated saved analytical view and return rows + audit event."""
    auth_context = authenticate_request(
        api_key_header=api_key_header,
        required_scope=SCOPE_QUERY,
        key_store=key_store,
    )

    rows: list[FundingTrendRow] | list[AllocationPeerRow] | list[HoldingsOverlapRow]

    if view_name == "funding_trend":
        rows = execute_funding_trend_view(view_inputs)  # type: ignore[arg-type]
    elif view_name == "allocation_peer_compare":
        if subject_plan_id is None or plan_period is None:
            msg = "allocation_peer_compare requires subject_plan_id and plan_period"
            raise ValueError(msg)
        rows = execute_allocation_peer_compare_view(
            view_inputs,  # type: ignore[arg-type]
            subject_plan_id=subject_plan_id,
            plan_period=plan_period,
        )
    elif view_name == "holdings_overlap":
        if subject_plan_id is None or plan_period is None:
            msg = "holdings_overlap requires subject_plan_id and plan_period"
            raise ValueError(msg)
        rows = execute_holdings_overlap_view(
            view_inputs,  # type: ignore[arg-type]
            subject_plan_id=subject_plan_id,
            plan_period=plan_period,
        )
    else:
        msg = f"unknown saved view: {view_name}"
        raise ValueError(msg)

    event_payload = dict(event or {})
    event_payload.update(
        {
            "view_name": view_name,
            "view_row_count": len(rows),
        }
    )

    return SavedViewRouteResult(
        view_name=view_name,
        rows=rows,
        audit_event=build_audit_event(
            operation="query.saved_view",
            auth_context=auth_context,
            event=event_payload,
        ),
    )
