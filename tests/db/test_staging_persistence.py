"""Tests for staged core metric DB persistence helpers."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from pension_data.db.migrations_runner import apply_migrations
from pension_data.db.staging_persistence import (
    _canonical_evidence,
    _canonical_instant,
    _row_values,
    _same_assertion,
    _same_assertion_source,
    persist_staging_core_metrics,
)
from pension_data.db.strategy import connect_database, resolve_database_config


def _evidence_row(evidence: object) -> dict[str, object]:
    return {
        "fact_id": "fact:evidence",
        "plan_id": "CA-PERS",
        "plan_period": "FY2024",
        "metric_family": "funded",
        "metric_name": "funded_ratio",
        "effective_date": "2024-06-30",
        "ingestion_date": "2026-03-03",
        "benchmark_version": "v1",
        "source_document_id": "doc:evidence",
        "evidence_refs": evidence,
    }


@pytest.mark.parametrize(
    ("evidence", "expected"),
    [
        (None, None),
        ('["p.40"]', '["p.40"]'),
        ({"amount": Decimal("123456789.12345678900")}, '{"amount": 123456789.123456789}'),
        ({"amount": Decimal("-0.00")}, '{"amount": 0}'),
        ({"values": [3, 0.25, True, None]}, '{"values": [3, 0.25, true, null]}'),
        ({2: "p.40"}, '{"2": "p.40"}'),
        ({None: "p.40"}, '{"null": "p.40"}'),
    ],
)
def test_persist_evidence_preserves_exact_json_and_is_idempotent(
    evidence: object, expected: str | None
) -> None:
    connection = connect_database(resolve_database_config(database_url="sqlite:///:memory:"))
    try:
        apply_migrations(connection, dialect="sqlite")
        row = _evidence_row(evidence)
        assert persist_staging_core_metrics(connection, dialect="sqlite", rows=[row]) == 1
        assert persist_staging_core_metrics(connection, dialect="sqlite", rows=[row]) == 0
        stored = connection.execute("SELECT evidence_refs FROM staging_core_metrics").fetchall()
        assert [item[0] for item in stored] == [expected]
    finally:
        connection.close()


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity"), float("nan"), float("inf")])
def test_persist_rejects_nonfinite_nested_evidence_and_rolls_back_batch(value: object) -> None:
    connection = connect_database(resolve_database_config(database_url="sqlite:///:memory:"))
    try:
        apply_migrations(connection, dialect="sqlite")
        valid = _evidence_row(["p.40"])
        invalid = {**_evidence_row({"values": [value]}), "fact_id": "fact:invalid"}
        with pytest.raises(ValueError, match="non-finite Decimal in evidence_refs"):
            persist_staging_core_metrics(connection, dialect="sqlite", rows=[valid, invalid])
        assert connection.execute("SELECT COUNT(*) FROM staging_core_metrics").fetchone()[0] == 0
    finally:
        connection.close()


@pytest.mark.parametrize("key", [("page", 40), frozenset({"page"})])
def test_persist_rejects_invalid_nested_evidence_keys_and_rolls_back_batch(key: object) -> None:
    connection = connect_database(resolve_database_config(database_url="sqlite:///:memory:"))
    try:
        apply_migrations(connection, dialect="sqlite")
        valid = _evidence_row(["p.40"])
        invalid = {**_evidence_row({"nested": {key: "p.41"}}), "fact_id": "fact:invalid"}
        with pytest.raises(TypeError, match="invalid JSON object key"):
            persist_staging_core_metrics(connection, dialect="sqlite", rows=[valid, invalid])
        assert connection.execute("SELECT COUNT(*) FROM staging_core_metrics").fetchone()[0] == 0
    finally:
        connection.close()


def test_empty_staging_batch_preserves_callers_pending_transaction() -> None:
    connection = connect_database(resolve_database_config(database_url="sqlite:///:memory:"))
    try:
        apply_migrations(connection, dialect="sqlite")
        connection.execute("CREATE TABLE pending_work (value TEXT)")
        connection.commit()
        connection.execute("INSERT INTO pending_work VALUES (?)", ("uncommitted",))

        assert persist_staging_core_metrics(connection, dialect="sqlite", rows=[]) == 0

        connection.rollback()
        assert connection.execute("SELECT value FROM pending_work").fetchall() == []
    finally:
        connection.close()


def test_persist_retries_legacy_text_evidence_and_assertion_dates_without_rewriting() -> None:
    connection = connect_database(resolve_database_config(database_url="sqlite:///:memory:"))
    try:
        apply_migrations(connection, dialect="sqlite")
        original = {**_evidence_row("p.40"), "ingestion_date": " legacy-import "}
        assert persist_staging_core_metrics(connection, dialect="sqlite", rows=[original]) == 1
        before = connection.execute("SELECT * FROM staging_core_metrics").fetchall()

        retry = {**original, "ingestion_date": "legacy-import"}
        assert persist_staging_core_metrics(connection, dialect="sqlite", rows=[retry]) == 0
        assert connection.execute("SELECT * FROM staging_core_metrics").fetchall() == before

        correction = {
            **original,
            "evidence_refs": "p.41",
            "ingestion_date": "legacy-correction",
            "source_document_id": "doc:correction",
        }
        assert persist_staging_core_metrics(connection, dialect="sqlite", rows=[correction]) == 1
        assert persist_staging_core_metrics(connection, dialect="sqlite", rows=[correction]) == 0
        stored = connection.execute(
            "SELECT evidence_refs, superseded_at FROM staging_core_metrics ORDER BY fact_id"
        ).fetchall()
        assert stored == [("p.40", "legacy-correction"), ("p.41", None)]
    finally:
        connection.close()


def test_historical_retry_requires_exact_closed_assertion_and_preserves_current_row() -> None:
    connection = connect_database(resolve_database_config(database_url="sqlite:///:memory:"))
    try:
        apply_migrations(connection, dialect="sqlite")
        original = _evidence_row(["p.40"])
        correction = {
            **original,
            "evidence_refs": ["p.41"],
            "ingestion_date": "2026-04-01",
            "source_document_id": "doc:correction",
        }
        latest = {
            **original,
            "evidence_refs": ["p.42"],
            "ingestion_date": "2026-05-01",
            "source_document_id": "doc:latest",
        }
        for row in [original, correction, latest]:
            assert persist_staging_core_metrics(connection, dialect="sqlite", rows=[row]) == 1
        before = connection.execute(
            "SELECT * FROM staging_core_metrics ORDER BY fact_id"
        ).fetchall()
        with pytest.raises(ValueError, match="conflicting assertion already exists"):
            persist_staging_core_metrics(connection, dialect="sqlite", rows=[correction])
        closed_correction = {**correction, "superseded_at": latest["ingestion_date"]}
        assert (
            persist_staging_core_metrics(connection, dialect="sqlite", rows=[closed_correction])
            == 0
        )
        with pytest.raises(ValueError, match="conflicting assertion already exists"):
            persist_staging_core_metrics(
                connection,
                dialect="sqlite",
                rows=[{**closed_correction, "evidence_refs": ["p.changed"]}],
            )
        after = connection.execute("SELECT * FROM staging_core_metrics ORDER BY fact_id").fetchall()
        assert [tuple(row) for row in after] == [tuple(row) for row in before]
        assert len(after) == 3
    finally:
        connection.close()


def test_persist_staging_core_metrics_rejects_same_source_conflict_in_sqlite() -> None:
    config = resolve_database_config(database_url="sqlite:///:memory:")
    connection = connect_database(config)
    try:
        apply_migrations(connection, dialect="sqlite")
        inserted = persist_staging_core_metrics(
            connection,
            dialect="sqlite",
            rows=[
                {
                    "fact_id": "fact:1",
                    "plan_id": "CA-PERS",
                    "plan_period": "FY2024",
                    "metric_family": "funded",
                    "metric_name": "funded_ratio",
                    "as_reported_value": 78.4,
                    "normalized_value": 0.784,
                    "as_reported_unit": "percent",
                    "normalized_unit": "ratio",
                    "manager_name": None,
                    "fund_name": None,
                    "vehicle_name": None,
                    "relationship_completeness": "complete",
                    "confidence": 0.9,
                    "evidence_refs": ["p.40"],
                    "effective_date": "2024-06-30",
                    "ingestion_date": "2026-03-03",
                    "benchmark_version": "v1",
                    "source_document_id": "doc:ca:2024",
                }
            ],
        )
        with pytest.raises(ValueError, match="conflicting assertion source"):
            persist_staging_core_metrics(
                connection,
                dialect="sqlite",
                rows=[
                    {
                        "fact_id": "fact:1",
                        "plan_id": "CA-PERS",
                        "plan_period": "FY2024",
                        "metric_family": "funded",
                        "metric_name": "funded_ratio",
                        "as_reported_value": 79.0,
                        "normalized_value": 0.79,
                        "as_reported_unit": "percent",
                        "normalized_unit": "ratio",
                        "manager_name": None,
                        "fund_name": None,
                        "vehicle_name": None,
                        "relationship_completeness": "complete",
                        "confidence": 0.92,
                        "evidence_refs": ["p.41"],
                        "effective_date": "2024-06-30",
                        "ingestion_date": "2026-03-03",
                        "benchmark_version": "v1",
                        "source_document_id": "doc:ca:2024",
                    }
                ],
            )
        row = connection.execute(
            "SELECT normalized_value, evidence_refs FROM staging_core_metrics WHERE fact_id = ?",
            ("fact:1",),
        ).fetchone()
    finally:
        connection.close()

    assert inserted == 1
    assert row is not None
    assert row[0] == 0.784
    assert json.loads(row[1]) == ["p.40"]


def test_persist_staging_core_metrics_rolls_back_batch_on_conflict_in_sqlite() -> None:
    config = resolve_database_config(database_url="sqlite:///:memory:")
    connection = connect_database(config)
    existing_row = {
        "fact_id": "fact:existing",
        "plan_id": "CA-PERS",
        "plan_period": "FY2024",
        "metric_family": "funded",
        "metric_name": "funded_ratio",
        "as_reported_value": 78.4,
        "normalized_value": 0.784,
        "as_reported_unit": "percent",
        "normalized_unit": "ratio",
        "manager_name": None,
        "fund_name": None,
        "vehicle_name": None,
        "relationship_completeness": "complete",
        "confidence": 0.9,
        "evidence_refs": ["p.40"],
        "effective_date": "2024-06-30",
        "ingestion_date": "2026-03-03",
        "benchmark_version": "v1",
        "source_document_id": "doc:ca:2024",
    }
    pending_row = {
        **existing_row,
        "fact_id": "fact:pending",
        "plan_id": "CA-TEACHERS",
        "source_document_id": "doc:ca:pending",
    }
    conflicting_row = {
        **existing_row,
        "as_reported_value": 79.0,
        "normalized_value": 0.79,
        "confidence": 0.92,
        "evidence_refs": ["p.41"],
    }
    try:
        apply_migrations(connection, dialect="sqlite")
        persist_staging_core_metrics(
            connection,
            dialect="sqlite",
            rows=[existing_row],
        )
        with pytest.raises(ValueError, match="conflicting assertion source"):
            persist_staging_core_metrics(
                connection,
                dialect="sqlite",
                rows=[pending_row, conflicting_row],
            )
        pending_count = connection.execute(
            "SELECT COUNT(*) FROM staging_core_metrics WHERE fact_id = ?",
            ("fact:pending",),
        ).fetchone()[0]
        existing_count = connection.execute(
            "SELECT COUNT(*) FROM staging_core_metrics WHERE fact_id = ?",
            ("fact:existing",),
        ).fetchone()[0]
    finally:
        connection.close()

    assert pending_count == 0
    assert existing_count == 1


def test_sqlite_retry_is_idempotent_with_timezone_equivalent_instants() -> None:
    """SQLite parity for AC retry counts 1,0,0 and Z/+00:00 creating no revision."""
    config = resolve_database_config(database_url="sqlite:///:memory:")
    connection = connect_database(config)
    row = {
        "fact_id": "fact:sqlite:retry:1",
        "plan_id": "CA-PERS-SQLITE-RETRY",
        "plan_period": "FY2024",
        "metric_family": "funded",
        "metric_name": "funded_ratio",
        "as_reported_value": 78.4,
        "normalized_value": 0.784,
        "as_reported_unit": "percent",
        "normalized_unit": "ratio",
        "manager_name": None,
        "fund_name": None,
        "vehicle_name": None,
        "relationship_completeness": "complete",
        "confidence": 0.95,
        "evidence_refs": ["p.40"],
        "effective_date": "2024-06-30T00:00:00Z",
        "ingestion_date": "2026-03-03T00:00:00Z",
        "benchmark_version": "v1",
        "source_document_id": "doc:ca:2024",
    }
    try:
        apply_migrations(connection, dialect="sqlite")
        first = persist_staging_core_metrics(connection, dialect="sqlite", rows=[row])
        second = persist_staging_core_metrics(connection, dialect="sqlite", rows=[row])
        third = persist_staging_core_metrics(connection, dialect="sqlite", rows=[row])
        tz_equivalent = persist_staging_core_metrics(
            connection,
            dialect="sqlite",
            rows=[
                {
                    **row,
                    "effective_date": "2024-06-30T00:00:00+00:00",
                    "ingestion_date": "2026-03-03T00:00:00+00:00",
                }
            ],
        )
        persisted_rows = connection.execute(
            """
            SELECT fact_id, superseded_at, restated
            FROM staging_core_metrics
            WHERE plan_id = ?
            """,
            ("CA-PERS-SQLITE-RETRY",),
        ).fetchall()
    finally:
        connection.close()

    assert (first, second, third) == (1, 0, 0)
    assert tz_equivalent == 0
    assert persisted_rows == [("fact:sqlite:retry:1", None, 0)]


def test_persist_staging_core_metrics_preserves_corrected_assertions_in_sqlite() -> None:
    config = resolve_database_config(database_url="sqlite:///:memory:")
    connection = connect_database(config)
    base_row = {
        "fact_id": "fact:restated",
        "plan_id": "CA-PERS",
        "plan_period": "FY2024",
        "metric_family": "funded",
        "metric_name": "funded_ratio",
        "as_reported_value": 78.4,
        "normalized_value": 0.784,
        "as_reported_unit": "percent",
        "normalized_unit": "ratio",
        "manager_name": None,
        "fund_name": None,
        "vehicle_name": None,
        "relationship_completeness": "complete",
        "confidence": 0.9,
        "evidence_refs": ["p.40"],
        "effective_date": "2024-06-30",
        "ingestion_date": "2026-03-03T00:00:00Z",
        "benchmark_version": "v1",
        "source_document_id": "doc:ca:2024-original",
    }
    corrected_row = {
        **base_row,
        "as_reported_value": 79.0,
        "normalized_value": 0.79,
        "confidence": 0.92,
        "evidence_refs": ["p.41"],
        "ingestion_date": "2026-04-01T00:00:00Z",
        "source_document_id": "doc:ca:2024-restated",
    }
    second_corrected_row = {
        **base_row,
        "as_reported_value": 80.0,
        "normalized_value": 0.8,
        "confidence": 0.94,
        "evidence_refs": ["p.42"],
        "ingestion_date": "2026-05-01T00:00:00Z",
        "source_document_id": "doc:ca:2024-restated-2",
    }
    try:
        apply_migrations(connection, dialect="sqlite")
        inserted = persist_staging_core_metrics(
            connection,
            dialect="sqlite",
            rows=[base_row],
        )
        restated_inserted = persist_staging_core_metrics(
            connection,
            dialect="sqlite",
            rows=[corrected_row],
        )
        retry_inserted = persist_staging_core_metrics(
            connection,
            dialect="sqlite",
            rows=[corrected_row],
        )
        second_restated_inserted = persist_staging_core_metrics(
            connection,
            dialect="sqlite",
            rows=[second_corrected_row],
        )
        rows = connection.execute(
            """
            SELECT fact_id, normalized_value, asserted_at, superseded_at, restated, evidence_refs
            FROM staging_core_metrics
            ORDER BY asserted_at, fact_id
            """,
        ).fetchall()
    finally:
        connection.close()

    assert inserted == 1
    assert restated_inserted == 1
    assert retry_inserted == 0
    assert second_restated_inserted == 1
    assert len(rows) == 3
    assert rows[0][0] == "fact:restated"
    assert rows[0][1] == 0.784
    assert rows[0][3] == "2026-04-01T00:00:00Z"
    assert rows[0][4] == 1
    assert json.loads(rows[0][5]) == ["p.40"]
    assert rows[1][0].startswith("fact:restated@2026-04-01T00:00:00Z:")
    assert rows[1][1] == 0.79
    assert rows[1][3] == "2026-05-01T00:00:00Z"
    assert rows[1][4] == 1
    assert json.loads(rows[1][5]) == ["p.41"]
    assert rows[2][0].startswith("fact:restated@2026-05-01T00:00:00Z:")
    assert rows[2][1] == 0.8
    assert rows[2][3] is None
    assert rows[2][4] == 1
    assert json.loads(rows[2][5]) == ["p.42"]


def test_canonical_helpers_normalize_evidence_and_utc_instants() -> None:
    assert _canonical_evidence(None) is None
    assert _canonical_evidence(["p.40", "p.41"]) == ["p.40", "p.41"]
    assert _canonical_evidence('["p.40", "p.41"]') == _canonical_evidence(["p.40", "p.41"])

    expected_utc = datetime(2026, 3, 3, 0, 0, tzinfo=UTC)
    assert _canonical_instant(None) is None
    assert _canonical_instant("") is None
    assert _canonical_instant("   ") is None
    assert _canonical_instant("2026-03-03T00:00:00Z") == expected_utc
    assert _canonical_instant("2026-03-03T00:00:00+00:00") == expected_utc
    assert _canonical_instant("2026-03-03T02:00:00+02:00") == expected_utc
    # SQLite staging rows often store date-only text for effective/ingestion dates.
    assert _canonical_instant("2026-03-03") == expected_utc
    assert _canonical_instant(date(2026, 3, 3)) == expected_utc
    assert _canonical_instant(datetime(2026, 3, 3, 0, 0)) == expected_utc
    assert (
        _canonical_instant(datetime(2026, 3, 3, 2, 0, tzinfo=timezone(timedelta(hours=2))))
        == expected_utc
    )

    candidate_row = {
        "fact_id": "fact:canon:1",
        "plan_id": "CA-PERS",
        "plan_period": "FY2024",
        "metric_family": "funded",
        "metric_name": "funded_ratio",
        "as_reported_value": 78.4,
        "normalized_value": 0.784,
        "as_reported_unit": "percent",
        "normalized_unit": "ratio",
        "manager_name": None,
        "fund_name": None,
        "vehicle_name": None,
        "relationship_completeness": "complete",
        "confidence": 0.95,
        "evidence_refs": ["p.40"],
        "effective_date": "2024-06-30T00:00:00+00:00",
        "ingestion_date": "2026-03-03T00:00:00+00:00",
        "valid_from": "2024-06-30T00:00:00Z",
        "valid_to": "2025-06-30T00:00:00Z",
        "asserted_at": "2026-03-03T00:00:00+00:00",
        "superseded_at": "2026-04-01T00:00:00Z",
        "restated": True,
        "benchmark_version": "v1",
        "source_document_id": "doc:ca:2024",
    }
    candidate_values = _row_values(candidate_row)

    pg_fetched_existing = (
        "fact:canon:1",
        "CA-PERS",
        "FY2024",
        "funded",
        "funded_ratio",
        78.4,
        0.784,
        "percent",
        "ratio",
        None,
        None,
        None,
        "complete",
        0.95,
        ["p.40"],
        datetime(2024, 6, 30, 0, 0, tzinfo=UTC),
        datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        datetime(2024, 6, 30, 0, 0, tzinfo=UTC),
        datetime(2025, 6, 30, 0, 0, tzinfo=UTC),
        datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        datetime(2026, 4, 1, 0, 0, tzinfo=UTC),
        True,
        "v1",
        "doc:ca:2024",
    )

    assert _same_assertion(pg_fetched_existing, candidate_values) is True
    assert _same_assertion_source(pg_fetched_existing, candidate_values) is True

    different_value_candidate = _row_values({**candidate_row, "normalized_value": 0.79})
    assert _same_assertion(pg_fetched_existing, different_value_candidate) is False
    assert _same_assertion_source(pg_fetched_existing, different_value_candidate) is True
