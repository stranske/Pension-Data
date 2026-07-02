"""Tests for bitemporal metric and holdings assertions."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from pension_data.db.models.bitemporal import (
    BitemporalAssertion,
    assert_no_active_valid_overlaps,
    query_as_known_at,
    supersede_assertions,
)
from pension_data.extract.investment.security_positions import (
    SecurityPositionInput,
    build_security_positions,
)


def test_metric_restatement_keeps_original_and_reproduces_known_at_views() -> None:
    original = BitemporalAssertion(
        entity_id="CA-PERS",
        fact_name="funded_ratio",
        value=0.74,
        valid_from="2023-06-30",
        valid_to="2024-06-30",
        asserted_at="2024-02-01T00:00:00Z",
        superseded_at="2025-03-01T00:00:00Z",
        source_document_id="doc:fy2023-original",
    )
    restated = BitemporalAssertion(
        entity_id="CA-PERS",
        fact_name="funded_ratio",
        value=0.78,
        valid_from="2023-06-30",
        valid_to="2024-06-30",
        asserted_at="2025-03-01T00:00:00Z",
        source_document_id="doc:fy2023-restated",
    )
    rows = [original, restated]

    as_known_then = query_as_known_at(
        rows,
        valid_at="2023-12-31",
        known_at="2024-12-31",
        key=lambda row: (row.entity_id, row.fact_name),
    )
    as_known_now = query_as_known_at(
        rows,
        valid_at="2023-12-31",
        known_at="2025-03-02",
        key=lambda row: (row.entity_id, row.fact_name),
    )

    assert [row.value for row in as_known_then] == [0.74]
    assert [row.value for row in as_known_now] == [0.78]
    assert original.is_restated
    assert not restated.is_restated
    assert_no_active_valid_overlaps(rows, key=lambda row: (row.entity_id, row.fact_name))


def test_active_overlap_guard_rejects_two_current_assertions_for_same_period() -> None:
    rows = [
        BitemporalAssertion(
            entity_id="CA-PERS",
            fact_name="funded_ratio",
            value=0.74,
            valid_from="2023-01-01",
            valid_to="2024-01-01",
            asserted_at="2024-02-01",
            source_document_id="doc:a",
        ),
        BitemporalAssertion(
            entity_id="CA-PERS",
            fact_name="funded_ratio",
            value=0.75,
            valid_from="2023-06-30",
            valid_to="2024-06-30",
            asserted_at="2024-02-02",
            source_document_id="doc:b",
        ),
    ]

    with pytest.raises(ValueError, match="active valid-time overlap"):
        assert_no_active_valid_overlaps(rows, key=lambda row: (row.entity_id, row.fact_name))


def test_13f_amendment_supersedes_security_position_without_losing_history() -> None:
    original = build_security_positions(
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
                provenance_ref="13f:original",
                asserted_at="2025-05-15T00:00:00Z",
            )
        ],
    )
    amendment = build_security_positions(
        plan_id="CA-PERS",
        plan_period="FY2025",
        rows=[
            SecurityPositionInput(
                security_name="APPLE INC",
                cusip="037833100",
                ticker=None,
                shares=2600.0,
                market_value_usd=1_300_000.0,
                asset_class="public_equity",
                source="13f",
                as_of="2025-03-31",
                provenance_ref="13f:amendment",
                asserted_at="2025-06-01T00:00:00Z",
                amendment_accession="0000919079-25-000001/A",
            )
        ],
    )
    rows = supersede_assertions(
        original,
        amendment,
        key=lambda row: (row.plan_id, row.security_id, row.source, row.as_of),
        superseded_at="2025-06-01T00:00:00Z",
    )

    before_amendment = query_as_known_at(
        rows,
        valid_at="2025-03-31",
        known_at="2025-05-20",
        key=lambda row: (row.plan_id, row.security_id, row.source),
    )
    after_amendment = query_as_known_at(
        rows,
        valid_at="2025-03-31",
        known_at="2025-06-02",
        key=lambda row: (row.plan_id, row.security_id, row.source),
    )

    assert [row.market_value_usd for row in before_amendment] == [1_250_000.0]
    assert [row.market_value_usd for row in after_amendment] == [1_300_000.0]
    assert rows[0].is_restated
    assert rows[1].amendment_accession == "0000919079-25-000001/A"
    assert_no_active_valid_overlaps(
        rows,
        key=lambda row: (row.plan_id, row.security_id, row.source),
    )


def test_supersede_assertions_rejects_invalid_superseded_at() -> None:
    rows = [
        BitemporalAssertion(
            entity_id="CA-PERS",
            fact_name="funded_ratio",
            value=0.74,
            valid_from="2023-06-30",
            valid_to="2024-06-30",
            asserted_at="2024-02-01T00:00:00Z",
            source_document_id="doc:fy2023-original",
        )
    ]

    with pytest.raises(ValueError, match="superseded_at"):
        supersede_assertions(
            rows,
            [
                BitemporalAssertion(
                    entity_id="CA-PERS",
                    fact_name="funded_ratio",
                    value=0.78,
                    valid_from="2023-06-30",
                    valid_to="2024-06-30",
                    asserted_at="2025-03-01T00:00:00Z",
                    source_document_id="doc:fy2023-restated",
                )
            ],
            key=lambda row: (row.entity_id, row.fact_name),
            superseded_at="not-a-date",
        )


def test_security_position_migration_rejects_active_overlaps() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    connection = sqlite3.connect(":memory:")
    connection.executescript(
        (
            repo_root / "src/pension_data/db/migrations/20260702_001_security_positions.sql"
        ).read_text(encoding="utf-8")
    )
    connection.executescript(
        (
            repo_root
            / "src/pension_data/db/migrations/20260702_002_security_positions_bitemporal.sql"
        ).read_text(encoding="utf-8")
    )
    base_row = {
        "plan_id": "CA-PERS",
        "plan_period": "FY2025",
        "security_id": "cusip:037833100",
        "security_name": "APPLE INC",
        "cusip": "037833100",
        "ticker": None,
        "shares": 2500.0,
        "market_value_usd": 1_250_000.0,
        "asset_class": "public_equity",
        "source": "13f",
        "as_of": "2025-03-31",
        "disclosure_state": "disclosed",
        "provenance_ref": "13f:original",
        "manager_name": None,
        "fund_name": None,
        "confidence": 1.0,
        "valid_from": "2025-03-31",
        "valid_to": "2025-06-30",
        "asserted_at": "2025-05-15T00:00:00Z",
        "superseded_at": None,
        "amendment_accession": None,
    }
    columns = tuple(base_row)
    placeholders = ", ".join("?" for _ in columns)
    connection.execute(
        f"INSERT INTO plan_security_positions ({', '.join(columns)}) VALUES ({placeholders})",
        tuple(base_row.values()),
    )

    overlapping = dict(base_row)
    overlapping["provenance_ref"] = "13f:amendment"
    overlapping["valid_from"] = "2025-04-01"
    overlapping["valid_to"] = "2025-05-01"
    overlapping["asserted_at"] = "2025-05-16T00:00:00Z"
    with pytest.raises(sqlite3.IntegrityError, match="active valid-time overlap"):
        connection.execute(
            f"INSERT INTO plan_security_positions ({', '.join(columns)}) VALUES ({placeholders})",
            tuple(overlapping.values()),
        )

    superseded_overlap = dict(overlapping)
    superseded_overlap["provenance_ref"] = "13f:superseded"
    superseded_overlap["superseded_at"] = "2025-05-20T00:00:00Z"
    connection.execute(
        f"INSERT INTO plan_security_positions ({', '.join(columns)}) VALUES ({placeholders})",
        tuple(superseded_overlap.values()),
    )

    non_overlapping = dict(base_row)
    non_overlapping["provenance_ref"] = "13f:next-quarter"
    non_overlapping["valid_from"] = "2025-07-01"
    non_overlapping["valid_to"] = "2025-09-30"
    non_overlapping["asserted_at"] = "2025-08-15T00:00:00Z"
    connection.execute(
        f"INSERT INTO plan_security_positions ({', '.join(columns)}) VALUES ({placeholders})",
        tuple(non_overlapping.values()),
    )

    with pytest.raises(sqlite3.IntegrityError, match="active valid-time overlap"):
        connection.execute(
            """
            UPDATE plan_security_positions
            SET valid_from = ?, valid_to = ?
            WHERE provenance_ref = ?
            """,
            ("2025-04-15", "2025-05-15", "13f:next-quarter"),
        )
