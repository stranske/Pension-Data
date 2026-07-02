"""Tests for CAFR-anchored holdings coverage reconciliation."""

from __future__ import annotations

import pytest

from pension_data.extract.investment.security_positions import (
    AcfrAllocationInput,
    build_security_positions,
    load_own_holdings_csv,
    parse_13f_information_table_xml,
    reconcile_holdings_to_acfr,
)


def test_security_holdings_reconcile_to_cafr_total_assets_and_asset_classes() -> None:
    own_holdings = load_own_holdings_csv(
        """security_name,cusip,ticker,shares,market_value_usd,asset_class
Apple Inc,037833100,AAPL,100,2500000,public_equity
US Treasury 10Y,91282CJL6,,50,1500000,fixed_income
""",
        as_of="2025-06-30",
        provenance_ref="calpers:own-holdings:fy2025",
    )
    thirteen_f = parse_13f_information_table_xml(
        """<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <infoTable>
    <nameOfIssuer>MICROSOFT CORP</nameOfIssuer>
    <cusip>594918104</cusip>
    <value>1000</value>
    <shrsOrPrnAmt><sshPrnamt>1200</sshPrnamt></shrsOrPrnAmt>
  </infoTable>
</informationTable>
""",
        as_of="2025-03-31",
        provenance_ref="edgar:calpers:2025q1",
    )
    positions = build_security_positions(
        plan_id="CA-PERS",
        plan_period="FY2025",
        rows=[*own_holdings, *thirteen_f],
    )

    report = reconcile_holdings_to_acfr(
        plan_id="CA-PERS",
        plan_period="FY2025",
        positions=positions,
        total_plan_assets_usd=20_000_000.0,
        acfr_allocations=[
            AcfrAllocationInput("public_equity", 10_000_000.0, "acfr:fy2025:p52"),
            AcfrAllocationInput("fixed_income", 5_000_000.0, "acfr:fy2025:p53"),
            AcfrAllocationInput("private_equity", 5_000_000.0, "acfr:fy2025:p54"),
        ],
    )

    assert report.collected_market_value_usd == 5_000_000.0
    assert report.coverage_ratio == 0.25
    assert report.scope_label == "equity-sleeve"
    assert report.by_asset_class["public_equity"] == 0.35
    assert report.by_asset_class["fixed_income"] == 0.3
    assert report.by_asset_class["private_equity"] == 0.0
    assert report.provenance_refs == (
        "calpers:own-holdings:fy2025",
        "edgar:calpers:2025q1",
        "acfr:fy2025:p52",
        "acfr:fy2025:p53",
        "acfr:fy2025:p54",
    )


def test_cafr_reconcile_rejects_non_positive_total_assets() -> None:
    with pytest.raises(ValueError, match="positive finite"):
        reconcile_holdings_to_acfr(
            plan_id="CA-PERS",
            plan_period="FY2025",
            positions=[],
            total_plan_assets_usd=0.0,
            acfr_allocations=[],
        )
