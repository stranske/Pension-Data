"""End-to-end holdings pipeline: own-file XLS, AB 2833 alts, and overlap view (#647).

Covers the pieces that surround 13F parsing:
- the own-holdings XLS loader (openpyxl optional dep -> importorskip);
- AB 2833 alts capture feeding total-plan coverage beyond the equity sleeve;
- the *real* ``execute_holdings_overlap_view`` run over ingested security-level
  positions (not hand-built view fixtures).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pension_data.extract.investment.security_positions import (
    Ab2833AltDisclosureInput,
    AcfrAllocationInput,
    SecurityPositionInput,
    ab2833_to_security_positions,
    build_security_positions,
    capture_ab2833_alt_disclosures,
    load_own_holdings_xls,
    reconcile_holdings_to_acfr,
)
from pension_data.query.saved_views.holdings_ingest import to_holdings_overlap_inputs
from pension_data.query.saved_views.service import execute_holdings_overlap_view


def _write_workbook(path: Path) -> None:
    openpyxl = pytest.importorskip("openpyxl")
    workbook = openpyxl.Workbook()
    worksheet = workbook.active
    worksheet.append(
        ["security_name", "cusip", "ticker", "shares", "market_value_usd", "asset_class"]
    )
    worksheet.append(["Apple Inc", "037833100", "AAPL", 100, 2_500_000, "public_equity"])
    worksheet.append(["US Treasury 10Y", "91282CJL6", None, 50, 1_500_000, "fixed_income"])
    worksheet.append([None, None, None, None, None, None])  # trailing blank row
    workbook.save(path)


def test_load_own_holdings_xls_from_path(tmp_path: Path) -> None:
    pytest.importorskip("openpyxl")
    workbook_path = tmp_path / "calpers_holdings.xlsx"
    _write_workbook(workbook_path)

    inputs = load_own_holdings_xls(
        workbook_path,
        as_of="2025-06-30",
        provenance_ref="calpers:own-holdings:fy2025",
    )

    assert len(inputs) == 2
    assert [row.cusip for row in inputs] == ["037833100", "91282CJL6"]
    assert inputs[0].ticker == "AAPL"
    assert inputs[0].market_value_usd == 2_500_000.0
    assert inputs[1].asset_class == "fixed_income"
    assert {row.source for row in inputs} == {"own_holdings_file"}


def test_load_own_holdings_xls_from_bytes(tmp_path: Path) -> None:
    pytest.importorskip("openpyxl")
    workbook_path = tmp_path / "calpers_holdings.xlsx"
    _write_workbook(workbook_path)

    inputs = load_own_holdings_xls(
        workbook_path.read_bytes(),
        as_of="2025-06-30",
        provenance_ref="calpers:own-holdings:fy2025",
    )
    assert len(inputs) == 2
    assert inputs[0].security_name == "Apple Inc"


def test_ab2833_alts_capture_feeds_total_plan_coverage() -> None:
    disclosures = capture_ab2833_alt_disclosures(
        [
            Ab2833AltDisclosureInput(
                fund_name="Blackstone Capital Partners VIII",
                asset_class="private_equity",
                reported_fair_value_usd=3_000_000.0,
                management_fees_usd=45_000.0,
                carried_interest_usd=120_000.0,
                as_of="2025-06-30",
                provenance_ref="ab2833:calpers:fy2025:pe",
                manager_name="Blackstone",
            ),
            Ab2833AltDisclosureInput(
                fund_name="Brookfield Infrastructure IV",
                asset_class="real_assets",
                reported_fair_value_usd=2_000_000.0,
                management_fees_usd=30_000.0,
                carried_interest_usd=None,
                as_of="2025-06-30",
                provenance_ref="ab2833:calpers:fy2025:ra",
                manager_name="Brookfield",
            ),
        ]
    )

    # Fees are summed under finite guards; the PE fund carries mgmt + carry.
    pe_disclosure = next(d for d in disclosures if d.asset_class == "private_equity")
    assert pe_disclosure.total_fees_usd == 165_000.0
    assert pe_disclosure.disclosure_state == "disclosed"

    alt_inputs = ab2833_to_security_positions(disclosures)
    equity_input = SecurityPositionInput(
        security_name="APPLE INC",
        cusip="037833100",
        ticker=None,
        shares=1000.0,
        market_value_usd=5_000_000.0,
        asset_class="public_equity",
        source="13f",
        as_of="2025-03-31",
        provenance_ref="edgar:calpers:2025q1",
    )
    positions = build_security_positions(
        plan_id="CA-PERS",
        plan_period="FY2025",
        rows=[equity_input, *alt_inputs],
    )

    report = reconcile_holdings_to_acfr(
        plan_id="CA-PERS",
        plan_period="FY2025",
        positions=positions,
        total_plan_assets_usd=40_000_000.0,
        acfr_allocations=[
            AcfrAllocationInput("public_equity", 20_000_000.0, "acfr:eq"),
            AcfrAllocationInput("private_equity", 6_000_000.0, "acfr:pe"),
            AcfrAllocationInput("real_assets", 4_000_000.0, "acfr:ra"),
            AcfrAllocationInput("fixed_income", 10_000_000.0, "acfr:fi"),
        ],
    )

    # Collected = 5M equity + 3M PE + 2M real assets = 10M of a 40M plan.
    assert report.collected_market_value_usd == 10_000_000.0
    assert report.coverage_ratio == 0.25
    assert report.scope_label == "equity-sleeve"
    # AB 2833 lets alts (which 13F never covers) contribute to coverage.
    assert report.by_asset_class["private_equity"] == 0.5
    assert report.by_asset_class["real_assets"] == 0.5
    assert report.by_asset_class["fixed_income"] == 0.0


def test_ab2833_alts_marks_explicit_and_missing_fair_value_as_not_disclosed() -> None:
    disclosures = capture_ab2833_alt_disclosures(
        [
            Ab2833AltDisclosureInput(
                fund_name="Explicitly withheld fund",
                asset_class="private_equity",
                reported_fair_value_usd=500_000.0,
                management_fees_usd=4_000.0,
                carried_interest_usd=6_000.0,
                as_of="2025-06-30",
                provenance_ref="ab2833:explicit-withheld",
                explicit_not_disclosed=True,
            ),
            Ab2833AltDisclosureInput(
                fund_name="Value omitted fund",
                asset_class="real_assets",
                reported_fair_value_usd=None,
                management_fees_usd=7_000.0,
                carried_interest_usd=None,
                as_of="2025-06-30",
                provenance_ref="ab2833:value-omitted",
            ),
        ]
    )

    explicit = next(
        disclosure
        for disclosure in disclosures
        if disclosure.provenance_ref == "ab2833:explicit-withheld"
    )
    missing_value = next(
        disclosure
        for disclosure in disclosures
        if disclosure.provenance_ref == "ab2833:value-omitted"
    )
    assert explicit.disclosure_state == "not_disclosed"
    assert explicit.reported_fair_value_usd == 500_000.0
    assert explicit.total_fees_usd == 10_000.0
    assert missing_value.disclosure_state == "not_disclosed"
    assert missing_value.reported_fair_value_usd is None
    assert missing_value.total_fees_usd == 7_000.0


def test_holdings_overlap_view_runs_over_ingested_security_positions() -> None:
    # Two plans both disclose the same CUSIP -> the real overlap view must pair them.
    subject = build_security_positions(
        plan_id="CA-PERS",
        plan_period="FY2025",
        rows=[
            SecurityPositionInput(
                security_name="APPLE INC",
                cusip="037833100",
                ticker=None,
                shares=2500.0,
                market_value_usd=1_250_000.0,
                asset_class="public_equity",
                source="13f",
                as_of="2025-03-31",
                provenance_ref="edgar:ca",
            ),
            SecurityPositionInput(
                security_name="NVIDIA CORP",
                cusip="67066G104",
                ticker=None,
                shares=1200.0,
                market_value_usd=4_500_000.0,
                asset_class="public_equity",
                source="13f",
                as_of="2025-03-31",
                provenance_ref="edgar:ca",
            ),
        ],
    )
    counterparty = build_security_positions(
        plan_id="NY-CRF",
        plan_period="FY2025",
        rows=[
            SecurityPositionInput(
                security_name="APPLE INC",
                cusip="037833100",
                ticker=None,
                shares=1000.0,
                market_value_usd=900_000.0,
                asset_class="public_equity",
                source="13f",
                as_of="2025-03-31",
                provenance_ref="edgar:ny",
            )
        ],
    )

    overlap_inputs = to_holdings_overlap_inputs([*subject, *counterparty])
    rows = execute_holdings_overlap_view(
        overlap_inputs,
        subject_plan_id="CA-PERS",
        plan_period="FY2025",
    )

    apple_rows = [row for row in rows if row.fund_name == "cusip:037833100"]
    assert len(apple_rows) == 1
    apple = apple_rows[0]
    assert apple.subject_plan_id == "CA-PERS"
    assert apple.counterparty_plan_id == "NY-CRF"
    assert apple.overlap_status == "overlap"
    assert apple.overlap_usd == 900_000.0  # min(1,250,000, 900,000)

    # NVIDIA is held only by the subject -> counterparty non-disclosure, no overlap.
    nvidia_rows = [row for row in rows if row.fund_name == "cusip:67066G104"]
    assert len(nvidia_rows) == 1
    assert nvidia_rows[0].overlap_status == "unknown_due_to_non_disclosure"
