"""Tests for investment allocation and fee normalization."""

from __future__ import annotations

from pension_data.normalize.investment_normalization import (
    normalize_allocation_category,
    normalize_amount_to_usd,
    normalize_rate_to_ratio,
)


class TestNormalizeAllocationCategory:
    def test_public_equity_aliases(self) -> None:
        assert normalize_allocation_category("public equity") == "public_equity"
        assert normalize_allocation_category("public equities") == "public_equity"
        assert normalize_allocation_category("equities") == "public_equity"

    def test_fixed_income_aliases(self) -> None:
        assert normalize_allocation_category("fixed income") == "fixed_income"
        assert normalize_allocation_category("bonds") == "fixed_income"

    def test_private_equity(self) -> None:
        assert normalize_allocation_category("private equity") == "private_equity"

    def test_real_assets_aliases(self) -> None:
        assert normalize_allocation_category("real assets") == "real_assets"
        assert normalize_allocation_category("real estate") == "real_assets"

    def test_cash(self) -> None:
        assert normalize_allocation_category("cash") == "cash"

    def test_case_insensitive(self) -> None:
        assert normalize_allocation_category("PUBLIC EQUITY") == "public_equity"
        assert normalize_allocation_category("Fixed Income") == "fixed_income"

    def test_unknown_label_passthrough_with_underscores(self) -> None:
        assert normalize_allocation_category("hedge funds") == "hedge_funds"
        assert normalize_allocation_category("infrastructure") == "infrastructure"

    def test_whitespace_normalization(self) -> None:
        assert normalize_allocation_category("  public   equity  ") == "public_equity"


class TestNormalizeRateToRatio:
    def test_percentage_divided_by_100(self) -> None:
        assert normalize_rate_to_ratio(75.0) == 0.75

    def test_ratio_passthrough(self) -> None:
        assert normalize_rate_to_ratio(0.75) == 0.75

    def test_boundary_at_one(self) -> None:
        assert normalize_rate_to_ratio(1.0) == 1.0

    def test_just_above_one_is_percentage(self) -> None:
        result = normalize_rate_to_ratio(1.5)
        assert result == 0.015

    def test_none_returns_none(self) -> None:
        assert normalize_rate_to_ratio(None) is None


class TestNormalizeAmountToUsd:
    def test_delegates_to_normalize_money_to_usd(self) -> None:
        assert normalize_amount_to_usd(5.0, unit_scale="million_usd") == 5_000_000.0

    def test_none_returns_none(self) -> None:
        assert normalize_amount_to_usd(None, unit_scale="usd") is None
