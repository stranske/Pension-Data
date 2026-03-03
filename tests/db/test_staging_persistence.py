"""Tests for staged core metric DB persistence helpers."""

from __future__ import annotations

import json

from pension_data.db.migrations_runner import apply_migrations
from pension_data.db.staging_persistence import persist_staging_core_metrics
from pension_data.db.strategy import connect_database, resolve_database_config


def test_persist_staging_core_metrics_upserts_rows_in_sqlite() -> None:
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
        _updated = persist_staging_core_metrics(
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
    assert row[0] == 0.79
    assert json.loads(row[1]) == ["p.41"]
