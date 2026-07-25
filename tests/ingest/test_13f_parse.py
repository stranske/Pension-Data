"""Tests for public 13F security-position ingestion."""

from __future__ import annotations

from pathlib import Path

import pytest

from pension_data.extract.investment.security_positions import (
    AcfrAllocationInput,
    SecurityPositionInput,
    build_security_positions,
    parse_13f_information_table_xml,
    reconcile_holdings_to_acfr,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures"
INFO_TABLE_FIXTURE = FIXTURE_DIR / "edgar_13f_information_table.xml"

# Recorded-shape EDGAR INFORMATION TABLE (egress sandboxed -> no live SEC call).
# Four holdings, values reported in thousands: 1250 + 2750 + 1500 + 4500 = 10000
# thousand USD -> 10,000,000 USD once scaled by the 13F thousands convention.
EXPECTED_POSITION_COUNT = 4
EXPECTED_MARKET_VALUE_SUM_USD = 10_000_000.0


def test_saved_13f_information_table_fixture_count_and_market_value_sum() -> None:
    inputs = parse_13f_information_table_xml(
        INFO_TABLE_FIXTURE.read_text(encoding="utf-8"),
        as_of="2025-03-31",
        provenance_ref="edgar:calpers:0000919079:2025q1",
    )
    positions = build_security_positions(
        plan_id="CA-PERS",
        plan_period="FY2025",
        rows=inputs,
    )

    assert len(positions) == EXPECTED_POSITION_COUNT
    assert (
        sum(position.market_value_usd or 0.0 for position in positions)
        == EXPECTED_MARKET_VALUE_SUM_USD
    )
    assert {position.source for position in positions} == {"13f"}
    assert {position.asset_class for position in positions} == {"public_equity"}
    assert all(position.as_of == "2025-03-31" for position in positions)
    assert all(
        position.provenance_ref == "edgar:calpers:0000919079:2025q1" for position in positions
    )


NAMESPACED_13F_XML = """<?xml version="1.0" encoding="UTF-8"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <infoTable>
    <nameOfIssuer>APPLE INC</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>037833100</cusip>
    <value>1250</value>
    <shrsOrPrnAmt>
      <sshPrnamt>2500</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
  </infoTable>
  <infoTable>
    <nameOfIssuer>MICROSOFT CORP</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>594918104</cusip>
    <value>2750</value>
    <shrsOrPrnAmt>
      <sshPrnamt>3000</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
  </infoTable>
</informationTable>
"""


def test_13f_information_table_parses_namespaced_xml_and_value_units() -> None:
    inputs = parse_13f_information_table_xml(
        NAMESPACED_13F_XML,
        as_of="2025-03-31",
        provenance_ref="edgar:calpers:2025q1",
    )

    positions = build_security_positions(
        plan_id="CA-PERS",
        plan_period="FY2025",
        rows=inputs,
    )

    assert len(positions) == 2
    assert [position.security_id for position in positions] == [
        "cusip:037833100",
        "cusip:594918104",
    ]
    assert sum(position.market_value_usd or 0.0 for position in positions) == 4_000_000.0
    assert positions[0].shares == 2500.0
    assert positions[0].source == "13f"
    assert positions[0].asset_class == "public_equity"
    assert positions[0].provenance_ref == "edgar:calpers:2025q1"


def test_security_positions_require_stable_identifier() -> None:
    rows = [
        SecurityPositionInput(
            security_name=None,
            cusip=None,
            ticker=None,
            shares=10.0,
            market_value_usd=100.0,
            asset_class="public_equity",
            source="own_holdings_file",
            as_of="2025-03-31",
            provenance_ref="own:missing-id",
        )
    ]

    with pytest.raises(ValueError, match="requires cusip, ticker, or security_name"):
        build_security_positions(plan_id="CA-PERS", plan_period="FY2025", rows=rows)


def test_holdings_coverage_provenance_refs_match_scoped_positions() -> None:
    matching = build_security_positions(
        plan_id="CA-PERS",
        plan_period="FY2025",
        rows=[
            SecurityPositionInput(
                security_name="APPLE INC",
                cusip="037833100",
                ticker=None,
                shares=10.0,
                market_value_usd=90.0,
                asset_class="public_equity",
                source="13f",
                as_of="2025-03-31",
                provenance_ref="edgar:ca",
            )
        ],
    )
    unrelated = build_security_positions(
        plan_id="NY-ERS",
        plan_period="FY2024",
        rows=[
            SecurityPositionInput(
                security_name="MICROSOFT CORP",
                cusip="594918104",
                ticker=None,
                shares=5.0,
                market_value_usd=10.0,
                asset_class="public_equity",
                source="13f",
                as_of="2024-03-31",
                provenance_ref="edgar:ny",
            )
        ],
    )

    report = reconcile_holdings_to_acfr(
        plan_id="CA-PERS",
        plan_period="FY2025",
        positions=[*matching, *unrelated],
        total_plan_assets_usd=100.0,
        acfr_allocations=[
            AcfrAllocationInput(
                asset_class="public_equity",
                market_value_usd=100.0,
                provenance_ref="acfr:ca",
            )
        ],
    )

    assert report.provenance_refs == ("edgar:ca", "acfr:ca")
    assert report.scope_label == "equity-sleeve"
