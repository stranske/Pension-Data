"""Adapt ingested security-level holdings into saved-view inputs (issue #647).

This is the seam that lets the *real* ``execute_holdings_overlap_view`` run over
collected 13F / own-file / AB 2833 positions instead of hand-built view fixtures.
Two plans "overlap" on an instrument when both disclose a holding keyed by the
same deterministic ``security_id`` (cusip -> ticker -> normalized name), so the
overlap view's tri-state disclosure logic carries straight through from the
ingested positions.
"""

from __future__ import annotations

from pension_data.db.models.investment_positions import PlanSecurityPosition
from pension_data.query.saved_views.models import DisclosureState, HoldingsOverlapInput

_VALID_DISCLOSURE_STATES: frozenset[str] = frozenset(
    {"disclosed", "not_disclosed", "known_not_invested"}
)


def to_holdings_overlap_inputs(
    positions: list[PlanSecurityPosition],
) -> list[HoldingsOverlapInput]:
    """Map security-level positions onto holdings-overlap view inputs.

    The instrument is projected onto the view's ``(manager_name, fund_name)`` key
    as ``(issuer name, security_id)``: ``security_id`` is the deterministic
    cusip/ticker/name identity produced by ``build_security_positions``, so the
    same instrument held by two plans lands on the same overlap key.
    """
    rows: list[HoldingsOverlapInput] = []
    for position in positions:
        disclosure_state: DisclosureState = (
            position.disclosure_state
            if position.disclosure_state in _VALID_DISCLOSURE_STATES
            else "not_disclosed"
        )
        rows.append(
            HoldingsOverlapInput(
                plan_id=position.plan_id,
                plan_period=position.plan_period,
                manager_name=position.security_name or position.security_id,
                fund_name=position.security_id,
                exposure_usd=position.market_value_usd,
                disclosure_state=disclosure_state,
            )
        )
    return rows
