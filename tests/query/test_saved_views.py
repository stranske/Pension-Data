"""Tests for initial saved analytical views."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from pension_data.query.saved_views import (
    AllocationPeerInput,
    FundingTrendInput,
    HoldingsOverlapInput,
    execute_allocation_peer_compare_view,
    execute_funding_trend_view,
    execute_holdings_overlap_view,
    load_saved_view_definitions,
)


def _seed_saved_view_sqlite() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.executescript("""
        CREATE TABLE plan_funding_metrics (
            plan_id TEXT NOT NULL,
            plan_period TEXT NOT NULL,
            funded_ratio REAL NOT NULL,
            employer_contributions_usd REAL,
            employee_contributions_usd REAL,
            benefit_payments_usd REAL
        );
        CREATE TABLE plan_allocation_metrics (
            plan_id TEXT NOT NULL,
            plan_period TEXT NOT NULL,
            peer_group TEXT NOT NULL,
            asset_class TEXT NOT NULL,
            allocation_pct REAL NOT NULL
        );
        CREATE TABLE manager_fund_positions (
            plan_id TEXT NOT NULL,
            plan_period TEXT NOT NULL,
            manager_name TEXT NOT NULL,
            fund_name TEXT NOT NULL,
            exposure_usd REAL,
            disclosure_state TEXT NOT NULL
        );
        """)
    connection.executemany(
        """
        INSERT INTO plan_funding_metrics (
            plan_id,
            plan_period,
            funded_ratio,
            employer_contributions_usd,
            employee_contributions_usd,
            benefit_payments_usd
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            ("CA-PERS", "FY2024", 0.78, 16_000_000.0, 6_000_000.0, 9_000_000.0),
            ("CA-PERS", "FY2025", 0.81, 18_000_000.0, 7_000_000.0, 10_000_000.0),
            ("TX-ERS", "FY2025", 0.72, 12_000_000.0, 5_000_000.0, 9_000_000.0),
        ],
    )
    connection.executemany(
        """
        INSERT INTO plan_allocation_metrics (
            plan_id,
            plan_period,
            peer_group,
            asset_class,
            allocation_pct
        ) VALUES (?, ?, ?, ?, ?)
        """,
        [
            ("CA-PERS", "FY2025", "large_public", "public_equity", 0.52),
            ("WA-RETIRE", "FY2025", "large_public", "public_equity", 0.47),
            ("TX-ERS", "FY2025", "large_public", "public_equity", 0.49),
            ("CA-PERS", "FY2025", "large_public", "fixed_income", 0.31),
            ("WA-RETIRE", "FY2025", "large_public", "fixed_income", 0.35),
            ("TX-ERS", "FY2025", "large_public", "fixed_income", 0.33),
        ],
    )
    connection.executemany(
        """
        INSERT INTO manager_fund_positions (
            plan_id,
            plan_period,
            manager_name,
            fund_name,
            exposure_usd,
            disclosure_state
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            ("CA-PERS", "FY2025", "Mercer", "Global Equity", 120_000_000.0, "disclosed"),
            ("CA-PERS", "FY2025", "Aon", "Private Credit", None, "not_disclosed"),
            ("TX-ERS", "FY2025", "Mercer", "Global Equity", 70_000_000.0, "disclosed"),
            ("TX-ERS", "FY2025", "Aon", "Private Credit", 20_000_000.0, "disclosed"),
            (
                "WA-RETIRE",
                "FY2025",
                "Mercer",
                "Global Equity",
                0.0,
                "known_not_invested",
            ),
        ],
    )
    return connection


def _required_definition() -> dict[str, object]:
    return {
        "view_name": "test_view",
        "version": "v1",
        "description": "test definition",
        "sql": "SELECT 1 AS sample_field;",
        "assumptions": ["test"],
        "output_schema": [{"name": "sample_field", "type": "number"}],
    }


def test_load_saved_view_definitions_and_validate_schema() -> None:
    definitions = load_saved_view_definitions(
        config_dir=Path(__file__).resolve().parents[2] / "config" / "saved_queries"
    )

    assert sorted(definitions) == [
        "allocation_peer_compare:v1",
        "funding_trend:v1",
        "holdings_overlap:v1",
    ]

    funding = definitions["funding_trend:v1"]
    assert funding.output_schema[0].name == "plan_id"
    assert funding.output_schema[3].name == "funded_ratio_change"

    overlap = definitions["holdings_overlap:v1"]
    assert any("not_disclosed" in item.lower() for item in overlap.assumptions)


def test_saved_view_sql_definitions_execute_against_seeded_data() -> None:
    definitions = load_saved_view_definitions(
        config_dir=Path(__file__).resolve().parents[2] / "config" / "saved_queries"
    )
    connection = _seed_saved_view_sqlite()
    try:
        funding_definition = definitions["funding_trend:v1"]
        funding_cursor = connection.execute(funding_definition.sql)
        funding_columns = [column[0] for column in funding_cursor.description or []]
        assert funding_columns == [field.name for field in funding_definition.output_schema]
        funding_rows = funding_cursor.fetchall()
        assert funding_rows
        plan_id_idx = funding_columns.index("plan_id")
        plan_period_idx = funding_columns.index("plan_period")
        funded_ratio_change_idx = funding_columns.index("funded_ratio_change")
        ca_pers_fy2025_row = next(
            (
                row
                for row in funding_rows
                if row[plan_id_idx] == "CA-PERS" and row[plan_period_idx] == "FY2025"
            ),
            None,
        )
        assert ca_pers_fy2025_row is not None
        assert ca_pers_fy2025_row[funded_ratio_change_idx] == pytest.approx(0.03)

        allocation_definition = definitions["allocation_peer_compare:v1"]
        allocation_cursor = connection.execute(
            allocation_definition.sql,
            {"plan_period": "FY2025", "subject_plan_id": "CA-PERS"},
        )
        allocation_columns = [column[0] for column in allocation_cursor.description or []]
        assert allocation_columns == [field.name for field in allocation_definition.output_schema]
        allocation_rows = allocation_cursor.fetchall()
        assert allocation_rows
        assert allocation_rows[0][5] == pytest.approx(0.34)

        overlap_definition = definitions["holdings_overlap:v1"]
        overlap_cursor = connection.execute(
            overlap_definition.sql,
            {"plan_period": "FY2025", "subject_plan_id": "CA-PERS"},
        )
        overlap_columns = [column[0] for column in overlap_cursor.description or []]
        assert overlap_columns == [field.name for field in overlap_definition.output_schema]
        overlap_rows = overlap_cursor.fetchall()
        assert overlap_rows
        assert any(row[5] == "overlap" and row[6] == 70_000_000.0 for row in overlap_rows)
    finally:
        connection.close()


def test_funding_trend_view_outputs_stable_order_and_ratio_deltas() -> None:
    rows = [
        FundingTrendInput("TX-ERS", "FY2025", 0.72, 12_000_000.0, 5_000_000.0, 9_000_000.0),
        FundingTrendInput("CA-PERS", "FY2025", 0.81, 18_000_000.0, 7_000_000.0, 10_000_000.0),
        FundingTrendInput("CA-PERS", "FY2024", 0.78, 16_000_000.0, 6_000_000.0, 9_000_000.0),
    ]

    output = execute_funding_trend_view(rows)

    assert [(row.plan_id, row.plan_period) for row in output] == [
        ("CA-PERS", "FY2024"),
        ("CA-PERS", "FY2025"),
        ("TX-ERS", "FY2025"),
    ]
    assert output[0].funded_ratio_change is None
    assert output[1].funded_ratio_change == 0.03
    assert output[1].net_external_cash_flow_usd == 15_000_000.0


def test_allocation_peer_compare_view_computes_peer_stats() -> None:
    rows = [
        AllocationPeerInput("CA-PERS", "FY2025", "large_public", "public_equity", 0.52),
        AllocationPeerInput("WA-RETIRE", "FY2025", "large_public", "public_equity", 0.47),
        AllocationPeerInput("TX-ERS", "FY2025", "large_public", "public_equity", 0.49),
        AllocationPeerInput("CA-PERS", "FY2025", "large_public", "fixed_income", 0.31),
        AllocationPeerInput("WA-RETIRE", "FY2025", "large_public", "fixed_income", 0.35),
        AllocationPeerInput("TX-ERS", "FY2025", "large_public", "fixed_income", 0.33),
    ]

    output = execute_allocation_peer_compare_view(
        rows,
        subject_plan_id="CA-PERS",
        plan_period="FY2025",
    )

    assert [row.asset_class for row in output] == ["fixed_income", "public_equity"]
    assert output[0].peer_mean_pct == 0.34
    assert output[0].delta_vs_peer_mean_pct == -0.03
    assert output[1].peer_median_pct == 0.48


def test_holdings_overlap_view_is_coverage_aware() -> None:
    rows = [
        HoldingsOverlapInput(
            "CA-PERS",
            "FY2025",
            "Mercer",
            "Global Equity",
            120_000_000.0,
            "disclosed",
        ),
        HoldingsOverlapInput(
            "CA-PERS",
            "FY2025",
            "Aon",
            "Private Credit",
            None,
            "not_disclosed",
        ),
        HoldingsOverlapInput(
            "TX-ERS",
            "FY2025",
            "Mercer",
            "Global Equity",
            70_000_000.0,
            "disclosed",
        ),
        HoldingsOverlapInput(
            "TX-ERS",
            "FY2025",
            "Aon",
            "Private Credit",
            20_000_000.0,
            "disclosed",
        ),
        HoldingsOverlapInput(
            "WA-RETIRE",
            "FY2025",
            "Mercer",
            "Global Equity",
            0.0,
            "known_not_invested",
        ),
    ]

    output = execute_holdings_overlap_view(
        rows,
        subject_plan_id="CA-PERS",
        plan_period="FY2025",
    )

    tx_mercer = [
        row
        for row in output
        if row.counterparty_plan_id == "TX-ERS"
        and row.manager_name == "Mercer"
        and row.fund_name == "Global Equity"
    ][0]
    tx_aon = [
        row
        for row in output
        if row.counterparty_plan_id == "TX-ERS"
        and row.manager_name == "Aon"
        and row.fund_name == "Private Credit"
    ][0]
    wa_mercer = [
        row
        for row in output
        if row.counterparty_plan_id == "WA-RETIRE"
        and row.manager_name == "Mercer"
        and row.fund_name == "Global Equity"
    ][0]
    wa_aon = [
        row
        for row in output
        if row.counterparty_plan_id == "WA-RETIRE"
        and row.manager_name == "Aon"
        and row.fund_name == "Private Credit"
    ][0]

    assert tx_mercer.overlap_status == "overlap"
    assert tx_mercer.overlap_usd == 70_000_000.0
    assert tx_aon.overlap_status == "unknown_due_to_non_disclosure"
    assert tx_aon.overlap_usd is None
    assert wa_mercer.overlap_status == "known_not_invested"
    assert wa_mercer.subject_disclosure_state == "disclosed"
    assert wa_mercer.counterparty_disclosure_state == "known_not_invested"
    assert wa_aon.overlap_status == "unknown_due_to_non_disclosure"
    assert wa_aon.counterparty_disclosure_state == "not_disclosed"


def test_load_saved_view_definitions_raises_when_config_dir_missing(tmp_path: Path) -> None:
    missing_dir = tmp_path / "missing-saved-views"
    with pytest.raises(FileNotFoundError, match="Saved view definition directory does not exist"):
        load_saved_view_definitions(config_dir=missing_dir)


def test_load_saved_view_definitions_raises_when_config_path_is_file(tmp_path: Path) -> None:
    config_file = tmp_path / "saved-views.json"
    config_file.write_text("{}", encoding="utf-8")
    with pytest.raises(NotADirectoryError, match="Saved view definition path is not a directory"):
        load_saved_view_definitions(config_dir=config_file)


def test_load_saved_view_definitions_raises_when_no_definition_artifacts(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="No saved view definition artifacts found"):
        load_saved_view_definitions(config_dir=tmp_path)


def test_load_saved_view_definitions_raises_for_empty_output_schema(tmp_path: Path) -> None:
    payload = _required_definition()
    payload["output_schema"] = []
    path = tmp_path / "test_view_v1.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="`output_schema` must not be empty"):
        load_saved_view_definitions(config_dir=tmp_path)


def test_load_saved_view_definitions_raises_for_duplicate_output_schema_names(
    tmp_path: Path,
) -> None:
    payload = _required_definition()
    payload["output_schema"] = [
        {"name": "sample_field", "type": "number"},
        {"name": "sample_field", "type": "number"},
    ]
    path = tmp_path / "test_view_v1.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="`output_schema` contains duplicate field names"):
        load_saved_view_definitions(config_dir=tmp_path)
