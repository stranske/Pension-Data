"""Saved analytical views and execution wrappers."""

from pension_data.query.saved_views.definitions import (
    SavedViewDefinition,
    SavedViewField,
    load_saved_view_definitions,
)
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

__all__ = [
    "AllocationPeerInput",
    "AllocationPeerRow",
    "FundingTrendInput",
    "FundingTrendRow",
    "HoldingsOverlapInput",
    "HoldingsOverlapRow",
    "SavedViewDefinition",
    "SavedViewField",
    "execute_allocation_peer_compare_view",
    "execute_funding_trend_view",
    "execute_holdings_overlap_view",
    "load_saved_view_definitions",
]
