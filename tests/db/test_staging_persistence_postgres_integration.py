"""Real PostgreSQL integration tests for staged core metric persistence."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from pension_data.db.migrations_runner import apply_migrations
from pension_data.db.staging_persistence import (
    _STAGING_CORE_METRIC_COLUMNS,
    _row_values,
    _same_assertion,
    _same_assertion_source,
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
    """Live JSONB/TIMESTAMPTZ fetches differ from _row_values under raw ==.

    Deliberate-break target: temporarily use raw ``==`` in
    ``staging_persistence._same_assertion`` / ``_same_assertion_source``.
    Issue #961's AC text mis-anchors the unchanged migration SQL; rewriting
    ``JSONB``/``TIMESTAMPTZ`` there without recreating the table does not trip
    this regression because ``CREATE TABLE IF NOT EXISTS`` leaves the live
    schema intact.
    """
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
    assert _same_assertion(fetched, candidate_values) is True, (
        "PostgreSQL JSONB/TIMESTAMPTZ fetches must compare equal after "
        "canonicalization; raw == is False for an identical assertion"
    )


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


def _staging_core_metrics_table_body() -> str:
    schema = (
        Path(__file__).parents[2]
        / "src/pension_data/db/migrations/20260303_101_pg_core_fact_staging.sql"
    ).read_text()
    return schema.split("CREATE TABLE IF NOT EXISTS staging_core_metrics (", 1)[1].split("\n);", 1)[
        0
    ]


def _declared_sql_type(table_body: str, column: str) -> str:
    """Return the complete SQL type token for a column (rejects array-prefix matches)."""
    for line in table_body.splitlines():
        stripped = line.strip().rstrip(",")
        if stripped.startswith(f"{column} "):
            return stripped[len(column) :].strip().split()[0]
    raise AssertionError(f"column {column!r} missing from staging_core_metrics")


# Audit #964: pure adapter controls run even when the live PostgreSQL DSN is absent.
def test_audit_inventories_equality_and_key_construction_seams() -> None:
    """Inventory every equality/key seam in the bounded persistence + holdings adapters."""
    from pension_data.db import staging_persistence as persistence
    from pension_data.extract.investment import security_positions as positions
    from pension_data.query.saved_views import holdings_ingest, service

    # producer → consumer seams that participate in semantic identity (#964 scope).
    seams: tuple[tuple[str, object], ...] = (
        ("persistence._serialize_evidence_refs", persistence._serialize_evidence_refs),
        ("persistence._row_values", persistence._row_values),
        ("persistence._canonical_evidence", persistence._canonical_evidence),
        ("persistence._canonical_instant", persistence._canonical_instant),
        ("persistence._canonical_column_value", persistence._canonical_column_value),
        ("persistence._same_assertion", persistence._same_assertion),
        ("persistence._same_assertion_source", persistence._same_assertion_source),
        ("persistence._replacement_fact_id", persistence._replacement_fact_id),
        ("persistence.persist_staging_core_metrics", persistence.persist_staging_core_metrics),
        ("positions._normalize_cusip", positions._normalize_cusip),
        ("positions._security_id", positions._security_id),
        ("positions.build_security_positions", positions.build_security_positions),
        ("holdings_ingest.to_holdings_overlap_inputs", holdings_ingest.to_holdings_overlap_inputs),
        ("service._aggregate_holdings_positions", service._aggregate_holdings_positions),
        ("service.execute_holdings_overlap_view", service.execute_holdings_overlap_view),
    )
    for name, fn in seams:
        assert callable(fn), name

    # Content equality is representation-tolerant; replacement key construction is not.
    # Disposition: review candidate — do not file an issue or repair in this audit PR.
    row_z = {
        **_base_pg_row(fact_id="audit:key:1", plan_id="AUDIT-KEY"),
        "asserted_at": "2026-03-03T00:00:00Z",
        "ingestion_date": "2026-03-03T00:00:00Z",
    }
    row_offset = {
        **row_z,
        "asserted_at": "2026-03-03T00:00:00+00:00",
        "ingestion_date": "2026-03-03T00:00:00+00:00",
    }
    values_z = _row_values(row_z)
    values_offset = _row_values(row_offset)
    assert values_z != values_offset
    assert _same_assertion(values_z, values_offset)
    assert persistence._replacement_fact_id(row_z) != persistence._replacement_fact_id(row_offset)

    # Direct builder callers may skip parser CUSIP normalization (unknown; human decision).
    punctuated = positions._normalize_cusip("037-833-100")
    assert punctuated == "037833100"
    assert positions._security_id(cusip="037-833-100", ticker=None, security_name=None) == (
        "cusip:037-833-100"
    )


@pytest.mark.parametrize(
    "column",
    ["effective_date", "ingestion_date", "valid_from", "valid_to", "asserted_at", "superseded_at"],
)
def test_audit_equivalent_assertion_shapes_and_true_conflicts(column: str) -> None:
    row = {
        **_base_pg_row(fact_id="audit:1", plan_id="AUDIT"),
        "valid_to": "2025-06-30T00:00:00Z",
        "superseded_at": "2026-04-01T00:00:00Z",
    }
    candidate = _row_values(row)
    fetched = list(candidate)
    index = _STAGING_CORE_METRIC_COLUMNS.index(column)
    instant = str(candidate[index])
    utc_instant = datetime.fromisoformat(instant.replace("Z", "+00:00")).astimezone(UTC)
    fetched[index] = utc_instant.astimezone(timezone(timedelta(hours=5, minutes=30)))
    fetched[_STAGING_CORE_METRIC_COLUMNS.index("evidence_refs")] = ["p.40"]
    assert _same_assertion(fetched, candidate)
    assert _same_assertion_source(fetched, candidate)
    fetched[index] = utc_instant + timedelta(seconds=1)
    assert not _same_assertion(fetched, candidate)
    assert _same_assertion_source(fetched, candidate) is (column != "asserted_at")
    fetched[index] = utc_instant
    fetched[_STAGING_CORE_METRIC_COLUMNS.index("normalized_value")] = 0.785
    assert not _same_assertion(fetched, candidate)
    # Content disagreement from the same document/assertion instant is a conflict,
    # not a new source. A different document is a distinct assertion source.
    assert _same_assertion_source(fetched, candidate)
    fetched[_STAGING_CORE_METRIC_COLUMNS.index("source_document_id")] = "doc:other"
    assert not _same_assertion_source(fetched, candidate)


@pytest.mark.parametrize(
    ("serialized", "decoded", "equivalent"),
    [
        ('["p.40", "p.41"]', ["p.40", "p.41"], True),
        ('["p.40", "p.41"]', ["p.41", "p.40"], False),
        ('["p.40", "p.41"]', ["p.40", "p.42"], False),
        ('["p.40", "p.40"]', ["p.40"], False),
        ('{"page": 40, "document": "annual"}', {"document": "annual", "page": 40}, True),
        ('{"page": 40, "document": "annual"}', {"document": "annual", "page": 41}, False),
        (None, [], False),
    ],
    ids=["ordered", "reordered", "changed", "duplicate", "object-order", "object-value", "missing"],
)
def test_audit_evidence_content_and_order_define_assertion_equality(
    serialized: str | None, decoded: object, equivalent: bool
) -> None:
    candidate = _row_values(
        {**_base_pg_row(fact_id="audit:evidence", plan_id="AUDIT"), "evidence_refs": serialized}
    )
    fetched = list(candidate)
    fetched[_STAGING_CORE_METRIC_COLUMNS.index("evidence_refs")] = decoded
    assert _same_assertion(fetched, candidate) is equivalent
    # Evidence disagreement affects content, never the assertion's source identity.
    assert _same_assertion_source(fetched, candidate)


def test_audit_schema_preserves_driver_representation_contract() -> None:
    """Static schema contract; this does not substitute for a live PG roundtrip."""
    core_table = _staging_core_metrics_table_body()
    assert _declared_sql_type(core_table, "evidence_refs") == "JSONB"
    for column in (
        "effective_date",
        "ingestion_date",
        "valid_from",
        "valid_to",
        "asserted_at",
        "superseded_at",
    ):
        assert _declared_sql_type(core_table, column) == "TIMESTAMPTZ"


def test_audit_security_identity_survives_names_and_separates_instruments() -> None:
    from pension_data.extract.investment.security_positions import (
        SecurityPositionInput,
        build_security_positions,
    )
    from pension_data.query.saved_views.holdings_ingest import to_holdings_overlap_inputs
    from pension_data.query.saved_views.service import execute_holdings_overlap_view

    def position(plan: str, cusip: str, name: str):
        return build_security_positions(
            plan_id=plan,
            plan_period="FY2024",
            rows=[
                SecurityPositionInput(
                    security_name=name,
                    cusip=cusip,
                    ticker=None,
                    shares=1,
                    market_value_usd=10,
                    asset_class="public_equity",
                    source="manual",
                    as_of="2024-06-30",
                    provenance_ref=f"doc:{plan}",
                )
            ],
        )[0]

    subject = position("A", "037833100", "APPLE INC")
    equivalent = position("B", "037833100", "Apple Incorporated")
    distinct = position("C", "594918104", "APPLE INC")
    adapted = to_holdings_overlap_inputs([subject, equivalent, distinct])
    assert adapted[0].manager_name == adapted[1].manager_name == "cusip:037833100"
    assert adapted[0].fund_name == adapted[1].fund_name
    assert adapted[0].display_name != adapted[1].display_name
    assert adapted[2].manager_name != adapted[0].manager_name
    result = execute_holdings_overlap_view(adapted, subject_plan_id="A", plan_period="FY2024")
    shared = [row for row in result if row.counterparty_plan_id == "B"]
    assert len(shared) == 1 and shared[0].overlap_usd == 10
    separate = [row for row in result if row.counterparty_plan_id == "C"]
    assert len(separate) == 2
    assert all(row.overlap_status == "unknown_due_to_non_disclosure" for row in separate)
