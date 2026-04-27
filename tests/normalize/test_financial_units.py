"""Tests for financial unit and sign-convention normalization."""

from __future__ import annotations

from pension_data.normalize.financial_units import normalize_flow_sign, normalize_money_to_usd


class TestNormalizeMoneyToUsd:
    def test_usd_passthrough(self) -> None:
        assert normalize_money_to_usd(100.0, unit_scale="usd") == 100.0

    def test_thousand_usd(self) -> None:
        assert normalize_money_to_usd(5.0, unit_scale="thousand_usd") == 5_000.0

    def test_million_usd(self) -> None:
        assert normalize_money_to_usd(2.5, unit_scale="million_usd") == 2_500_000.0

    def test_billion_usd(self) -> None:
        assert normalize_money_to_usd(1.25, unit_scale="billion_usd") == 1_250_000_000.0

    def test_none_amount_returns_none(self) -> None:
        assert normalize_money_to_usd(None, unit_scale="million_usd") is None

    def test_rounding_to_six_decimals(self) -> None:
        result = normalize_money_to_usd(1.1234567, unit_scale="usd")
        assert result == 1.123457


class TestNormalizeFlowSign:
    def test_inflow_is_positive(self) -> None:
        assert (
            normalize_flow_sign(100.0, direction="inflow", outflows_reported_as_negative=False)
            == 100.0
        )

    def test_inflow_negative_input_becomes_positive(self) -> None:
        assert (
            normalize_flow_sign(-100.0, direction="inflow", outflows_reported_as_negative=False)
            == 100.0
        )

    def test_outflow_is_negative(self) -> None:
        assert (
            normalize_flow_sign(100.0, direction="outflow", outflows_reported_as_negative=False)
            == -100.0
        )

    def test_outflow_already_negative_when_reported_as_negative(self) -> None:
        assert (
            normalize_flow_sign(-100.0, direction="outflow", outflows_reported_as_negative=True)
            == -100.0
        )

    def test_outflow_positive_when_reported_as_negative_gets_negated(self) -> None:
        assert (
            normalize_flow_sign(100.0, direction="outflow", outflows_reported_as_negative=True)
            == -100.0
        )

    def test_balance_keeps_raw_sign(self) -> None:
        assert (
            normalize_flow_sign(-50.0, direction="balance", outflows_reported_as_negative=False)
            == -50.0
        )
        assert (
            normalize_flow_sign(50.0, direction="balance", outflows_reported_as_negative=False)
            == 50.0
        )

    def test_none_amount_returns_none(self) -> None:
        assert (
            normalize_flow_sign(None, direction="inflow", outflows_reported_as_negative=False)
            is None
        )
