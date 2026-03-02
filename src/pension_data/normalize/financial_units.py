"""Financial unit and sign-convention normalization for flow extraction."""

from __future__ import annotations

from typing import Literal

UnitScale = Literal["usd", "thousand_usd", "million_usd", "billion_usd"]
FlowDirection = Literal["inflow", "outflow", "balance"]

_UNIT_MULTIPLIER: dict[UnitScale, float] = {
    "usd": 1.0,
    "thousand_usd": 1_000.0,
    "million_usd": 1_000_000.0,
    "billion_usd": 1_000_000_000.0,
}


def normalize_money_to_usd(amount: float | None, *, unit_scale: UnitScale) -> float | None:
    """Normalize a reported amount into USD based on declared unit scale."""
    if amount is None:
        return None
    return round(amount * _UNIT_MULTIPLIER[unit_scale], 6)


def normalize_flow_sign(
    amount_usd: float | None,
    *,
    direction: FlowDirection,
    outflows_reported_as_negative: bool,
) -> float | None:
    """Normalize component sign conventions to a deterministic representation.

    Inflows are always positive, outflows are always negative, balances keep raw sign.
    """
    if amount_usd is None:
        return None
    if direction == "balance":
        return amount_usd

    magnitude = abs(amount_usd)
    if direction == "inflow":
        return magnitude

    if outflows_reported_as_negative:
        return amount_usd if amount_usd <= 0 else -magnitude
    return -magnitude
