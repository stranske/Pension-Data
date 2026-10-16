"""Real PostgreSQL integration tests for staged core metric persistence."""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

import pytest

from pension_data.db.migrations_runner import apply_migrations
from pension_data.db.staging_persistence import (
    _STAGING_CORE_METRIC_COLUMNS,
    _row_values,
    _same_assertion,
    persist_staging_core_metrics,
)
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


def _base_pg_row(*, fact_id: str, plan_id: str) -> dict[str, object]:
    return {
        "fact_id": fact_id,
        "plan_id": plan_id,
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


def _reset_plan_rows(connection: Any, *plan_ids: str) -> None:
    for plan_id in plan_ids:
        connection.execute("DELETE FROM staging_core_metrics WHERE plan_id = %s", (plan_id,))
    connection.commit()


def test_staging_core_metric_rows_roundtrip_on_postgres() -> None:
    connection = _postgres_connection()
    try:
        _reset_plan_rows(connection, "CA-PERS")
        inserted = persist_staging_core_metrics(
            connection,
            dialect="postgresql",
            rows=[_base_pg_row(fact_id="fact:pg:1", plan_id="CA-PERS")],
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


def test_postgres_fetched_row_shape_requires_canonical_equality() -> None:
    """Live JSONB/TIMESTAMPTZ fetches differ from _row_values under raw ==."""
    connection = _postgres_connection()
    row = _base_pg_row(fact_id="fact:pg:shape:1", plan_id="CA-PERS-SHAPE")
    columns = ", ".join(_STAGING_CORE_METRIC_COLUMNS)
    try:
        _reset_plan_rows(connection, "CA-PERS-SHAPE")
        inserted = persist_staging_core_metrics(connection, dialect="postgresql", rows=[row])
        fetched = connection.execute(
            f"SELECT {columns} FROM staging_core_metrics WHERE fact_id = %s",
            ("fact:pg:shape:1",),
        ).fetchone()
    finally:
        connection.close()

    assert inserted == 1
    assert fetched is not None
    candidate_values = _row_values(row)
    evidence_index = _STAGING_CORE_METRIC_COLUMNS.index("evidence_refs")
    asserted_at_index = _STAGING_CORE_METRIC_COLUMNS.index("asserted_at")

    assert isinstance(fetched[evidence_index], list)
    assert isinstance(candidate_values[evidence_index], str)
    assert fetched[evidence_index] != candidate_values[evidence_index]

    assert isinstance(fetched[asserted_at_index], datetime)
    assert isinstance(candidate_values[asserted_at_index], str)
    assert fetched[asserted_at_index] != candidate_values[asserted_at_index]

    assert (
        all(fetched[index] == candidate for index, candidate in enumerate(candidate_values))
        is False
    )
    assert _same_assertion(fetched, candidate_values) is True


def test_postgres_retry_is_idempotent() -> None:
    connection = _postgres_connection()
    row = _base_pg_row(fact_id="fact:pg:retry:1", plan_id="CA-PERS-RETRY")
    try:
        _reset_plan_rows(connection, "CA-PERS-RETRY")
        first = persist_staging_core_metrics(connection, dialect="postgresql", rows=[row])
        second = persist_staging_core_metrics(connection, dialect="postgresql", rows=[row])
        third = persist_staging_core_metrics(connection, dialect="postgresql", rows=[row])
        tz_equivalent = persist_staging_core_metrics(
            connection,
            dialect="postgresql",
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
            WHERE plan_id = %s
            """,
            ("CA-PERS-RETRY",),
        ).fetchall()
    finally:
        connection.close()

    assert (first, second, third) == (1, 0, 0)
    assert tz_equivalent == 0
    assert persisted_rows == [("fact:pg:retry:1", None, False)]


def test_postgres_same_source_conflict_rolls_back_batch() -> None:
    connection = _postgres_connection()
    existing_row = {
        **_base_pg_row(fact_id="fact:pg:conflict:1", plan_id="CA-PERS-CONFLICT"),
        "asserted_at": "2026-03-03T00:00:00Z",
    }
    pending_row = {
        **_base_pg_row(fact_id="fact:pg:conflict:pending", plan_id="CA-TEACHERS-CONFLICT"),
        "source_document_id": "doc:ca:2024:pending",
        "asserted_at": "2026-03-03T00:00:00Z",
    }
    conflicting_row = {
        **existing_row,
        "as_reported_value": 79.0,
        "normalized_value": 0.79,
        "confidence": 0.92,
        "evidence_refs": ["p.41"],
        "asserted_at": "2026-03-03T00:00:00+00:00",
    }
    try:
        _reset_plan_rows(connection, "CA-PERS-CONFLICT", "CA-TEACHERS-CONFLICT")
        inserted = persist_staging_core_metrics(
            connection,
            dialect="postgresql",
            rows=[existing_row],
        )
        with pytest.raises(ValueError, match="conflicting assertion source"):
            persist_staging_core_metrics(
                connection,
                dialect="postgresql",
                rows=[pending_row, conflicting_row],
            )
        pending_count = connection.execute(
            "SELECT COUNT(*) FROM staging_core_metrics WHERE fact_id = %s",
            ("fact:pg:conflict:pending",),
        ).fetchone()[0]
        existing_state = connection.execute(
            """
            SELECT normalized_value, superseded_at, restated
            FROM staging_core_metrics
            WHERE plan_id = %s
            """,
            ("CA-PERS-CONFLICT",),
        ).fetchall()
    finally:
        connection.close()

    assert inserted == 1
    assert pending_count == 0
    assert existing_state == [(0.784, None, False)]


def test_postgres_newer_assertion_creates_one_replacement() -> None:
    connection = _postgres_connection()
    base_row = {
        **_base_pg_row(fact_id="fact:pg:replace:1", plan_id="CA-PERS-REPLACE"),
        "source_document_id": "doc:ca:2024-original",
    }
    newer_row = {
        **base_row,
        "as_reported_value": 79.0,
        "normalized_value": 0.79,
        "confidence": 0.92,
        "evidence_refs": ["p.41"],
        "ingestion_date": "2026-04-01T00:00:00Z",
        "source_document_id": "doc:ca:2024-restated",
    }
    try:
        _reset_plan_rows(connection, "CA-PERS-REPLACE")
        inserted = persist_staging_core_metrics(
            connection,
            dialect="postgresql",
            rows=[base_row],
        )
        replaced = persist_staging_core_metrics(
            connection,
            dialect="postgresql",
            rows=[newer_row],
        )
        repeated = persist_staging_core_metrics(
            connection,
            dialect="postgresql",
            rows=[newer_row],
        )
        rows = connection.execute(
            """
            SELECT fact_id, normalized_value, superseded_at, restated, evidence_refs
            FROM staging_core_metrics
            WHERE plan_id = %s
            ORDER BY asserted_at, fact_id
            """,
            ("CA-PERS-REPLACE",),
        ).fetchall()
    finally:
        connection.close()

    assert (inserted, replaced, repeated) == (1, 1, 0)
    assert len(rows) == 2
    assert rows[0][0] == "fact:pg:replace:1"
    assert rows[0][1] == 0.784
    assert rows[0][2] is not None
    assert rows[0][3] is True
    assert rows[1][0].startswith("fact:pg:replace:1@2026-04-01T00:00:00Z:")
    assert rows[1][1] == 0.79
    assert rows[1][2] is None
    assert rows[1][3] is True
