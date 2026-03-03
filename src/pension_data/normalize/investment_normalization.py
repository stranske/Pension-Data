"""Normalization helpers for investment allocation and fee extraction."""

from __future__ import annotations

from pension_data.normalize.financial_units import UnitScale, normalize_money_to_usd

_ALLOCATION_CATEGORY_ALIASES: dict[str, str] = {
    "public equity": "public_equity",
    "public equities": "public_equity",
    "equities": "public_equity",
    "fixed income": "fixed_income",
    "bonds": "fixed_income",
    "private equity": "private_equity",
    "real assets": "real_assets",
    "real estate": "real_assets",
    "cash": "cash",
}


def normalize_allocation_category(label: str) -> str:
    """Normalize allocation labels into stable snake_case categories."""
    normalized = " ".join(label.strip().lower().split())
    if normalized in _ALLOCATION_CATEGORY_ALIASES:
        return _ALLOCATION_CATEGORY_ALIASES[normalized]
    return normalized.replace(" ", "_")


def normalize_rate_to_ratio(rate: float | None) -> float | None:
    """Normalize fee or contribution rates into ratio units."""
    if rate is None:
        return None
    if rate > 1.0:
        return round(rate / 100.0, 9)
    return round(rate, 9)


def normalize_amount_to_usd(amount: float | None, *, unit_scale: UnitScale) -> float | None:
    """Normalize optional monetary amount into USD."""
    return normalize_money_to_usd(amount, unit_scale=unit_scale)
