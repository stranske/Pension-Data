"""Tests for the EDGAR 13F-HR fetch flow (issue #647).

Egress is sandboxed, so the network ``_get`` path is never exercised; the URL
builders and the submissions-JSON selector are the tested units, and the
end-to-end flow is driven from recorded-shape fixtures under ``fixtures/``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pension_data.extract.investment.security_positions import (
    build_security_positions,
    parse_13f_information_table_xml,
)
from pension_data.sources.edgar import (
    EdgarApiError,
    EdgarClient,
    build_information_table_url,
    build_submissions_url,
    format_cik,
    parse_13f_hr_filings,
    select_latest_13f_hr,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures"
SUBMISSIONS_FIXTURE = FIXTURE_DIR / "edgar_submissions_CIK0000919079.json"
INFO_TABLE_FIXTURE = FIXTURE_DIR / "edgar_13f_information_table.xml"

CALPERS_CIK = "0000919079"


def test_format_cik_zero_pads_to_ten_digits() -> None:
    assert format_cik(919079) == "0000919079"
    assert format_cik("919079") == "0000919079"
    assert format_cik("0000919079") == "0000919079"
    assert format_cik("CIK0000810265") == "0000810265"


def test_format_cik_rejects_non_numeric_and_overlong() -> None:
    with pytest.raises(EdgarApiError, match="numeric"):
        format_cik("not-a-cik")
    with pytest.raises(EdgarApiError, match="exceeds 10 digits"):
        format_cik("123456789012")


def test_build_submissions_url_uses_ten_digit_cik() -> None:
    assert build_submissions_url(919079) == "https://data.sec.gov/submissions/CIK0000919079.json"


def test_build_information_table_url_uses_unpadded_cik_and_stripped_accession() -> None:
    url = build_information_table_url(
        919079,
        accession_number="0000919079-25-000042",
        document="form13fInfoTable.xml",
    )
    assert url == (
        "https://www.sec.gov/Archives/edgar/data/919079/" "000091907925000042/form13fInfoTable.xml"
    )


def test_information_table_url_requires_a_document() -> None:
    with pytest.raises(EdgarApiError, match="document name is required"):
        build_information_table_url(919079, accession_number="0000919079-25-000042", document="  ")


def test_select_latest_13f_hr_from_saved_submissions() -> None:
    submissions_json = SUBMISSIONS_FIXTURE.read_text(encoding="utf-8")
    latest = select_latest_13f_hr(submissions_json)
    # The NPORT-P filing must be ignored, and the newest 13F-HR wins by filingDate.
    assert latest.form == "13F-HR"
    assert latest.accession_number == "0000919079-25-000042"
    assert latest.filing_date == "2025-05-15"
    assert latest.report_date == "2025-03-31"

    all_13f = parse_13f_hr_filings(submissions_json)
    assert [filing.accession_number for filing in all_13f] == [
        "0000919079-25-000042",
        "0000919079-25-000019",
        "0000919079-24-000088",
    ]


def test_select_latest_13f_hr_raises_when_plan_files_none() -> None:
    # A plan that outsources equity management and files no 13F-HR.
    with pytest.raises(EdgarApiError, match="no 13F-HR filings"):
        select_latest_13f_hr('{"filings": {"recent": {"form": ["NPORT-P"]}}}')


def test_edgar_client_requires_user_agent() -> None:
    with pytest.raises(EdgarApiError, match="User-Agent is required"):
        EdgarClient(user_agent="   ")


def test_edgar_client_builds_flow_urls() -> None:
    client = EdgarClient(user_agent="Pension-Data research tim@stranskemo.com")
    assert (
        client.submissions_url(CALPERS_CIK) == "https://data.sec.gov/submissions/CIK0000919079.json"
    )
    latest = select_latest_13f_hr(SUBMISSIONS_FIXTURE.read_text(encoding="utf-8"))
    url = client.information_table_url(
        CALPERS_CIK,
        accession_number=latest.accession_number,
        document="form13fInfoTable.xml",
    )
    assert url.endswith("/919079/000091907925000042/form13fInfoTable.xml")


def test_end_to_end_fixture_flow_produces_security_positions() -> None:
    # 1. submissions JSON -> latest 13F-HR selection
    latest = select_latest_13f_hr(SUBMISSIONS_FIXTURE.read_text(encoding="utf-8"))
    # 2. information table XML (would be fetched from the built URL on a live host)
    provenance_ref = f"edgar:{CALPERS_CIK}:{latest.accession_number}"
    inputs = parse_13f_information_table_xml(
        INFO_TABLE_FIXTURE.read_text(encoding="utf-8"),
        as_of=latest.report_date,
        provenance_ref=provenance_ref,
    )
    positions = build_security_positions(plan_id="CA-PERS", plan_period="FY2025", rows=inputs)

    assert len(positions) == 4
    assert sum(position.market_value_usd or 0.0 for position in positions) == 10_000_000.0
    assert all(position.as_of == "2025-03-31" for position in positions)
    assert all(position.provenance_ref == provenance_ref for position in positions)
