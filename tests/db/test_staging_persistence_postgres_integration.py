"""Real PostgreSQL integration tests for staged core metric persistence."""

from __future__ import annotations

import json
import os
from typing import Any

import pytest

from pension_data.db.migrations_runner import apply_migrations
from pension_data.db.staging_persistence import persist_staging_core_metrics
from pension_data.db.strategy import connect_database, resolve_database_config

POSTGRES_TEST_URL_ENV = "PENSION_DATA_TEST_POSTGRES_URL"


def _postgres_connection() -> Any:
    database_url = os.getenv(POSTGRES_TEST_URL_ENV)
    if not database_url:
        pytest.skip(f"{POSTGRES_TEST_URL_ENV} is not set")
    config = resolve_database_config(environment="production", database_url=database_url)
    connection = connect_database(config)
    apply_migrations(connection, dialect=config.dialect)
    return connection


def test_staging_core_metric_rows_roundtrip_on_postgres() -> None:
    connection = _postgres_connection()
    try:
        inserted = persist_staging_core_metrics(
            connection,
            dialect="postgresql",
            rows=[
                {
                    "fact_id": "fact:pg:1",
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
                    "effective_date": "2024-06-30T00:00:00Z",
                    "ingestion_date": "2026-03-03T00:00:00Z",
                    "benchmark_version": "v1",
                    "source_document_id": "doc:ca:2024",
                }
            ],
        )
        row = connection.execute(
            "SELECT normalized_value, evidence_refs FROM staging_core_metrics WHERE fact_id = %s",
            ("fact:pg:1",),
        ).fetchone()
    finally:
        connection.close()

    assert inserted == 1
    assert row is not None
    assert row[0] == 0.784
    evidence_refs = row[1]
    if isinstance(evidence_refs, str):
        assert json.loads(evidence_refs) == ["p.40"]
    else:
        assert evidence_refs == ["p.40"]
