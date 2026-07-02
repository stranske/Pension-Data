"""Tests for public 13F security-position ingestion."""

from __future__ import annotations

from pension_data.extract.investment.security_positions import (
    build_security_positions,
    parse_13f_information_table_xml,
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
