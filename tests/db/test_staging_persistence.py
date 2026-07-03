"""Tests for staged core metric DB persistence helpers."""

from __future__ import annotations

import json

from pension_data.db.migrations_runner import apply_migrations
from pension_data.db.staging_persistence import persist_staging_core_metrics
from pension_data.db.strategy import connect_database, resolve_database_config


def test_persist_staging_core_metrics_keeps_existing_assertion_in_sqlite() -> None:
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
        duplicate = persist_staging_core_metrics(
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
    assert duplicate == 0
    assert row is not None
    assert row[0] == 0.784
    assert json.loads(row[1]) == ["p.40"]


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
    assert len(rows) == 2
    assert rows[0][0] == "fact:restated"
    assert rows[0][1] == 0.784
    assert rows[0][3] == "2026-04-01T00:00:00Z"
    assert rows[0][4] == 1
    assert json.loads(rows[0][5]) == ["p.40"]
    assert rows[1][0].startswith("fact:restated@2026-04-01T00:00:00Z:")
    assert rows[1][1] == 0.79
    assert rows[1][3] is None
    assert rows[1][4] == 1
    assert json.loads(rows[1][5]) == ["p.41"]
