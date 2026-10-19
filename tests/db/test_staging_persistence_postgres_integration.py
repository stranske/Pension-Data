"""Real PostgreSQL integration tests for staged core metric persistence."""

from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime, timedelta, timezone
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
from pension_data.db.strategy import DatabaseDialect, connect_database, resolve_database_config

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


@pytest.mark.parametrize("dialect", ["sqlite", "postgresql"])
@pytest.mark.parametrize(
    ("document_id", "same_source"),
    [
        pytest.param("doc:ca:2024", True, id="same-document"),
        pytest.param("doc:CA:2024", False, id="case-distinct-document"),
        pytest.param(" doc:ca:2024 ", False, id="space-distinct-document"),
    ],
)
def test_audit_document_identity_controls_conflict_or_restatement(
    dialect: DatabaseDialect, document_id: str, same_source: bool
) -> None:
    """Source IDs compare exactly even when assertion instants compare canonically."""
    if dialect == "postgresql":
        connection = _postgres_connection()
    else:
        connection = connect_database(resolve_database_config(database_url="sqlite:///:memory:"))
        apply_migrations(connection, dialect=dialect)
    base = _base_pg_row(fact_id="audit:document:1", plan_id="AUDIT-DOCUMENT")
    changed = {
        **base,
        "source_document_id": document_id,
        "asserted_at": "2026-03-03T02:00:00+02:00",
        "normalized_value": 0.79,
        "as_reported_value": 79.0,
    }
    snapshot_sql = (
        f"SELECT {', '.join(_STAGING_CORE_METRIC_COLUMNS)} FROM staging_core_metrics "
        "WHERE plan_id = 'AUDIT-DOCUMENT' ORDER BY fact_id"
    )
    try:
        if dialect == "postgresql":
            _reset_plan_rows(connection, "AUDIT-DOCUMENT")
        assert persist_staging_core_metrics(connection, dialect=dialect, rows=[base]) == 1
        before = connection.execute(snapshot_sql).fetchall()
        assert not _same_assertion(before[0], _row_values(changed))
        assert _same_assertion_source(before[0], _row_values(changed)) is same_source
        if same_source:
            with pytest.raises(ValueError, match="conflicting assertion source"):
                persist_staging_core_metrics(connection, dialect=dialect, rows=[changed])
            assert connection.execute(snapshot_sql).fetchall() == before
        else:
            assert persist_staging_core_metrics(connection, dialect=dialect, rows=[changed]) == 1
            after = connection.execute(snapshot_sql).fetchall()
            assert len(after) == 2
            historical, active = after
            assert historical[_STAGING_CORE_METRIC_COLUMNS.index("superseded_at")] is not None
            assert active[_STAGING_CORE_METRIC_COLUMNS.index("superseded_at")] is None
            assert active[_STAGING_CORE_METRIC_COLUMNS.index("source_document_id")] == document_id
            assert active[_STAGING_CORE_METRIC_COLUMNS.index("normalized_value")] == 0.79
            assert persist_staging_core_metrics(connection, dialect=dialect, rows=[changed]) == 0
            assert connection.execute(snapshot_sql).fetchall() == after
    finally:
        connection.close()


@pytest.mark.parametrize("dialect", ["sqlite", "postgresql"])
@pytest.mark.parametrize(
    ("base_evidence", "evidence_refs", "equivalent"),
    [
        pytest.param(["p.40", "p.41"], '[ "p.40", "p.41" ]', True, id="json-whitespace"),
        pytest.param(["p.40", "p.41"], '["p.41", "p.40"]', False, id="reordered"),
        pytest.param(["p.40", "p.41"], '["p.40", "p.42"]', False, id="changed-reference"),
        pytest.param(["p.40", "p.41"], '["p.40", "p.41", "p.41"]', False, id="duplicate-reference"),
        pytest.param(None, None, True, id="missing-retry"),
        pytest.param([], "[ ]", True, id="empty-list-retry"),
        pytest.param(None, "[]", False, id="missing-to-empty"),
        pytest.param([], None, False, id="empty-to-missing"),
        pytest.param(
            {"page": 40, "document": "annual"},
            '{"document": "annual", "page": 40}',
            True,
            id="object-key-order",
        ),
        pytest.param(
            {"page": 40, "document": "annual"},
            '{"document": "annual", "page": 41}',
            False,
            id="object-value",
        ),
    ],
)
def test_audit_evidence_only_retry_preserves_content_contract(
    dialect: DatabaseDialect, base_evidence: object, evidence_refs: object, equivalent: bool
) -> None:
    """Preserve evidence order, object contents, and absence across persistence retries."""
    if dialect == "postgresql":
        connection = _postgres_connection()
    else:
        connection = connect_database(resolve_database_config(database_url="sqlite:///:memory:"))
        apply_migrations(connection, dialect=dialect)
    base = {
        **_base_pg_row(fact_id="audit:evidence-retry:1", plan_id="AUDIT-EVIDENCE-RETRY"),
        "evidence_refs": base_evidence,
    }
    retry = {**base, "evidence_refs": evidence_refs}
    snapshot_sql = (
        f"SELECT {', '.join(_STAGING_CORE_METRIC_COLUMNS)} FROM staging_core_metrics "
        "WHERE plan_id = 'AUDIT-EVIDENCE-RETRY' ORDER BY fact_id"
    )
    try:
        if dialect == "postgresql":
            _reset_plan_rows(connection, "AUDIT-EVIDENCE-RETRY")
        assert persist_staging_core_metrics(connection, dialect=dialect, rows=[base]) == 1
        before = connection.execute(snapshot_sql).fetchall()
        if equivalent:
            assert persist_staging_core_metrics(connection, dialect=dialect, rows=[retry]) == 0
        else:
            with pytest.raises(
                ValueError, match="^conflicting assertion source for 'audit:evidence-retry:1'$"
            ):
                persist_staging_core_metrics(connection, dialect=dialect, rows=[retry])
        assert connection.execute(snapshot_sql).fetchall() == before
        # A conflict must leave the original assertion available for an exact retry.
        assert persist_staging_core_metrics(connection, dialect=dialect, rows=[base]) == 0
        assert connection.execute(snapshot_sql).fetchall() == before
    finally:
        connection.close()


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
            tokens = stripped[len(column) :].strip().split()
            constraint_words = {
                "NOT",
                "NULL",
                "DEFAULT",
                "PRIMARY",
                "UNIQUE",
                "CHECK",
                "REFERENCES",
                "COLLATE",
            }
            boundary = next(
                (i for i, token in enumerate(tokens) if token.upper() in constraint_words),
                len(tokens),
            )
            return " ".join(tokens[:boundary])
    raise AssertionError(f"column {column!r} missing from staging_core_metrics")


# Audit #964: local adapter controls run even when the live PostgreSQL DSN is absent.
@pytest.mark.parametrize("dialect", ["sqlite", "postgresql"])
@pytest.mark.parametrize(
    "retry_overrides",
    [
        pytest.param({}, id="identical"),
        pytest.param({"ingestion_date": "2026-04-01T02:00:00+02:00"}, id="timezone"),
        pytest.param({"evidence_refs": '["p.41"]'}, id="json-text"),
        pytest.param({"asserted_at": "2026-04-01T00:00:00Z"}, id="explicit-default"),
        pytest.param({"as_reported_value": 79}, id="numeric-int"),
    ],
)
def test_audit_restatement_retry_representation_boundary(
    dialect: DatabaseDialect, retry_overrides: dict[str, object]
) -> None:
    """Representation-equivalent restatement retries are idempotent (#964).

    SQLite exercises the real persistence path locally. PostgreSQL remains an
    independent live check; a local pass does not prove PG parity.
    """
    if dialect == "postgresql":
        connection = _postgres_connection()
    else:
        connection = connect_database(resolve_database_config(database_url="sqlite:///:memory:"))
        apply_migrations(connection, dialect=dialect)
    base = _base_pg_row(fact_id="audit:restatement:1", plan_id="AUDIT-RESTATEMENT")
    restatement = {
        **base,
        "normalized_value": 0.79,
        "as_reported_value": 79.0,
        "evidence_refs": ["p.41"],
        "ingestion_date": "2026-04-01T00:00:00Z",
        "source_document_id": "doc:restated",
    }
    retry = {**restatement, **retry_overrides}
    pending = _base_pg_row(fact_id="audit:restatement:pending", plan_id="AUDIT-RESTATEMENT-PENDING")
    snapshot_sql = (
        f"SELECT {', '.join(_STAGING_CORE_METRIC_COLUMNS)} FROM staging_core_metrics "
        "WHERE plan_id IN ('AUDIT-RESTATEMENT', 'AUDIT-RESTATEMENT-PENDING') ORDER BY fact_id"
    )
    assert _same_assertion(_row_values(restatement), _row_values(retry))
    assert _same_assertion_source(_row_values(restatement), _row_values(retry))
    try:
        if dialect == "postgresql":
            _reset_plan_rows(connection, "AUDIT-RESTATEMENT", "AUDIT-RESTATEMENT-PENDING")
        assert persist_staging_core_metrics(connection, dialect=dialect, rows=[base]) == 1
        assert persist_staging_core_metrics(connection, dialect=dialect, rows=[restatement]) == 1
        # Exact retries provide a passing control before varying only representation.
        assert persist_staging_core_metrics(connection, dialect=dialect, rows=[restatement]) == 0
        before_retry = connection.execute(snapshot_sql).fetchall()
        # A prior write in the same batch proves the equivalent retry does not
        # create a duplicate replacement or disturb the stored assertions.
        inserted = persist_staging_core_metrics(connection, dialect=dialect, rows=[pending, retry])
        assert inserted == 1  # Only the pending fact is new when the retry succeeds.
        after_retry = connection.execute(snapshot_sql).fetchall()
        assert [row for row in after_retry if row[1] == "AUDIT-RESTATEMENT"] == [
            row for row in before_retry if row[1] == "AUDIT-RESTATEMENT"
        ]
        count = connection.execute(
            "SELECT COUNT(*) FROM staging_core_metrics WHERE plan_id = 'AUDIT-RESTATEMENT'"
        ).fetchone()[0]
        assert count == 2
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("first", "equivalent", "distinct"),
    [(79.0, 79, 80), (-0.0, 0, 1), (float(2**53), 2**53, 2**53 + 1)],
)
def test_replacement_key_numeric_equivalence_preserves_distinct_values(
    first: float, equivalent: int, distinct: int
) -> None:
    from pension_data.db.staging_persistence import _replacement_fact_id

    row = _base_pg_row(fact_id="audit:numeric-key:1", plan_id="AUDIT-NUMERIC-KEY")
    key = _replacement_fact_id({**row, "as_reported_value": first})
    assert key == _replacement_fact_id({**row, "as_reported_value": equivalent})
    assert key != _replacement_fact_id({**row, "as_reported_value": distinct})
    assert _replacement_fact_id({**row, "as_reported_value": True}) != _replacement_fact_id(
        {**row, "as_reported_value": 1}
    )


@pytest.mark.parametrize(
    ("first", "equivalent", "distinct"),
    [
        pytest.param("79.0", 79, 80, id="decimal-integral-vs-int"),
        pytest.param("79.00", 79.0, 80.0, id="decimal-integral-vs-float"),
        pytest.param("0.790", "0.79", "0.80", id="decimal-trailing-zero"),
        pytest.param("9007199254740993", 2**53 + 1, 2**53, id="decimal-large-int-exact"),
    ],
)
def test_replacement_key_decimal_equivalence_preserves_distinct_values(
    first: str, equivalent: object, distinct: object
) -> None:
    from decimal import Decimal

    from pension_data.db.staging_persistence import _replacement_fact_id

    row = _base_pg_row(fact_id="audit:decimal-key:1", plan_id="AUDIT-DECIMAL-KEY")
    first_value = Decimal(first)
    equivalent_value = Decimal(equivalent) if isinstance(equivalent, str) else equivalent
    distinct_value = Decimal(distinct) if isinstance(distinct, str) else distinct
    assert _same_assertion(
        _row_values({**row, "as_reported_value": first_value}),
        _row_values({**row, "as_reported_value": equivalent_value}),
    )
    key = _replacement_fact_id({**row, "as_reported_value": first_value})
    assert key == _replacement_fact_id({**row, "as_reported_value": equivalent_value})
    assert key != _replacement_fact_id({**row, "as_reported_value": distinct_value})
    assert _replacement_fact_id({**row, "as_reported_value": Decimal("1")}) != (
        _replacement_fact_id({**row, "as_reported_value": True})
    )


@pytest.mark.parametrize(
    ("first", "equivalent", "distinct"),
    [
        pytest.param({"page": 40}, {"page": 40.0}, {"page": 41}, id="object-value"),
        pytest.param(
            {"refs": [40, 41]}, {"refs": [40.0, 41.0]}, {"refs": [40, 42]}, id="nested-list"
        ),
        pytest.param(
            {"page": 40, "items": [{"n": 2}]},
            '{"items": [{"n": 2.0}], "page": 40.0}',
            {"page": 40, "items": [{"n": 3}]},
            id="json-text-nested",
        ),
    ],
)
def test_replacement_key_evidence_numeric_equivalence_preserves_distinct_values(
    first: object, equivalent: object, distinct: object
) -> None:
    from pension_data.db.staging_persistence import _replacement_fact_id

    row = _base_pg_row(fact_id="audit:evidence-numeric-key:1", plan_id="AUDIT-EVIDENCE-KEY")
    key = _replacement_fact_id({**row, "evidence_refs": first})
    assert _same_assertion(
        _row_values({**row, "evidence_refs": first}),
        _row_values({**row, "evidence_refs": equivalent}),
    )
    assert key == _replacement_fact_id({**row, "evidence_refs": equivalent})
    assert key != _replacement_fact_id({**row, "evidence_refs": distinct})
    # JSON booleans retain a distinct identity from numbers, as at the top level.
    assert _replacement_fact_id({**row, "evidence_refs": {"flag": True}}) != _replacement_fact_id(
        {**row, "evidence_refs": {"flag": 1}}
    )


@pytest.mark.parametrize(
    ("first", "other", "equivalent"),
    [
        pytest.param({"flag": True}, {"flag": 1}, False, id="evidence-true-vs-one"),
        pytest.param({"flag": False}, {"flag": 0}, False, id="evidence-false-vs-zero"),
        pytest.param({"flag": True}, {"flag": True}, True, id="evidence-true-control"),
        pytest.param({"page": 1}, {"page": 1.0}, True, id="evidence-numeric-control"),
    ],
)
def test_same_assertion_bool_identity_matches_replacement_key(
    first: object, other: object, equivalent: bool
) -> None:
    """JSON true/false must not collapse into 1/0 in content equality (#964).

    The replacement key already distinguishes them; ``_same_assertion`` must
    share that contract or a true->1 evidence change is dropped as a retry.
    """
    from pension_data.db.staging_persistence import _replacement_fact_id

    row = _base_pg_row(fact_id="audit:bool-key:1", plan_id="AUDIT-BOOL-KEY")
    assert (
        _same_assertion(
            _row_values({**row, "evidence_refs": first}),
            _row_values({**row, "evidence_refs": other}),
        )
        is equivalent
    )
    assert (
        _replacement_fact_id({**row, "evidence_refs": first})
        == _replacement_fact_id({**row, "evidence_refs": other})
    ) is equivalent
    # Typed relational columns keep driver equivalence: SQLite returns the
    # restated flag as int 1 where the candidate carries Python True.
    fetched = list(_row_values({**row, "restated": True}))
    fetched[_STAGING_CORE_METRIC_COLUMNS.index("restated")] = 1
    assert _same_assertion(tuple(fetched), _row_values({**row, "restated": True}))


@pytest.mark.parametrize("dialect", ["sqlite", "postgresql"])
def test_audit_bool_evidence_restatement_is_not_dropped_as_retry(
    dialect: DatabaseDialect,
) -> None:
    """A true->1 evidence change from a new source creates one replacement."""
    if dialect == "postgresql":
        connection = _postgres_connection()
    else:
        connection = connect_database(resolve_database_config(database_url="sqlite:///:memory:"))
        apply_migrations(connection, dialect=dialect)
    base = {
        **_base_pg_row(fact_id="audit:bool-restatement:1", plan_id="AUDIT-BOOL-RESTATEMENT"),
        "evidence_refs": {"verified": True},
    }
    restatement = {
        **base,
        "evidence_refs": {"verified": 1},
        "ingestion_date": "2026-04-01T00:00:00Z",
        "source_document_id": "doc:restated",
    }
    try:
        if dialect == "postgresql":
            _reset_plan_rows(connection, "AUDIT-BOOL-RESTATEMENT")
        assert persist_staging_core_metrics(connection, dialect=dialect, rows=[base]) == 1
        assert persist_staging_core_metrics(connection, dialect=dialect, rows=[restatement]) == 1
        assert persist_staging_core_metrics(connection, dialect=dialect, rows=[restatement]) == 0
        count = connection.execute(
            "SELECT COUNT(*) FROM staging_core_metrics " "WHERE plan_id = 'AUDIT-BOOL-RESTATEMENT'"
        ).fetchone()[0]
        assert count == 2
    finally:
        connection.close()


@pytest.mark.parametrize("dialect", ["sqlite", "postgresql"])
def test_audit_evidence_numeric_restatement_retry_is_idempotent(
    dialect: DatabaseDialect,
) -> None:
    """A 40 vs 40.0 evidence-object retry must not raise a same-source conflict."""
    if dialect == "postgresql":
        connection = _postgres_connection()
    else:
        connection = connect_database(resolve_database_config(database_url="sqlite:///:memory:"))
        apply_migrations(connection, dialect=dialect)
    base = {
        **_base_pg_row(fact_id="audit:evidence-numeric:1", plan_id="AUDIT-EVIDENCE-NUMERIC"),
        "evidence_refs": {"page": 39},
    }
    restatement = {
        **base,
        "normalized_value": 0.79,
        "as_reported_value": 79.0,
        "evidence_refs": {"page": 40},
        "ingestion_date": "2026-04-01T00:00:00Z",
        "source_document_id": "doc:restated",
    }
    retry = {**restatement, "evidence_refs": {"page": 40.0}}
    try:
        if dialect == "postgresql":
            _reset_plan_rows(connection, "AUDIT-EVIDENCE-NUMERIC")
        assert persist_staging_core_metrics(connection, dialect=dialect, rows=[base]) == 1
        assert persist_staging_core_metrics(connection, dialect=dialect, rows=[restatement]) == 1
        assert persist_staging_core_metrics(connection, dialect=dialect, rows=[retry]) == 0
        count = connection.execute(
            "SELECT COUNT(*) FROM staging_core_metrics WHERE plan_id = 'AUDIT-EVIDENCE-NUMERIC'"
        ).fetchone()[0]
        assert count == 2
    finally:
        connection.close()


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
        ("persistence._canonical_key_value", persistence._canonical_key_value),
        ("persistence._canonical_assertion_value", persistence._canonical_assertion_value),
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

    # Content equality and replacement key construction share the assertion
    # contract: representation-equivalent rows derive one replacement key,
    # while a genuine content change derives a different key (#964).
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
    assert persistence._replacement_fact_id(row_z) == persistence._replacement_fact_id(row_offset)
    assert persistence._replacement_fact_id(
        {**row_z, "evidence_refs": '["p.40"]'}
    ) == persistence._replacement_fact_id(row_z)
    assert persistence._replacement_fact_id(
        {**row_z, "normalized_value": 0.785}
    ) != persistence._replacement_fact_id(row_z)

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


@pytest.mark.parametrize("column", ["valid_from", "asserted_at"])
@pytest.mark.parametrize("missing_value", [None, ""], ids=["none", "empty"])
def test_audit_temporal_defaults_match_explicit_instants(
    column: str, missing_value: object
) -> None:
    """Default validity/assertion instants keep their separate content/source roles."""
    row = _base_pg_row(fact_id="audit:default-time", plan_id="AUDIT")
    omitted = _row_values(row)
    candidate = _row_values({**row, column: missing_value})
    source_column = "effective_date" if column == "valid_from" else "ingestion_date"
    instant = datetime.fromisoformat(str(row[source_column]).replace("Z", "+00:00"))
    explicit = _row_values(
        {**row, column: instant.astimezone(timezone(timedelta(hours=-7))).isoformat()}
    )
    assert candidate == omitted
    assert _same_assertion(explicit, candidate)
    assert _same_assertion_source(explicit, candidate)

    changed = _row_values({**row, column: (instant + timedelta(microseconds=1)).isoformat()})
    assert not _same_assertion(changed, candidate)
    assert _same_assertion_source(changed, candidate) is (column != "asserted_at")


@pytest.mark.parametrize(
    "asserted_at",
    [
        date(2026, 3, 3),
        "2026-03-03",
        datetime(2026, 3, 3),
        "2026-03-03T00:00:00",
        " 2026-03-03T00:00:00z ",
    ],
    ids=["date", "date-text", "naive-datetime", "naive-text", "padded-lowercase-z"],
)
def test_audit_date_and_naive_assertions_use_utc(asserted_at: object) -> None:
    """The comparison contract interprets date-only and naive assertions as UTC."""
    candidate = _row_values(
        {
            **_base_pg_row(fact_id="audit:date", plan_id="AUDIT"),
            "asserted_at": asserted_at,
        }
    )
    fetched = list(candidate)
    index = _STAGING_CORE_METRIC_COLUMNS.index("asserted_at")
    fetched[index] = datetime(2026, 3, 3, tzinfo=UTC)
    assert _same_assertion(fetched, candidate)
    assert _same_assertion_source(fetched, candidate)

    # A genuinely different instant changes both content and source identity.
    fetched[index] = datetime(2026, 3, 3, tzinfo=UTC) + timedelta(microseconds=1)
    assert not _same_assertion(fetched, candidate)
    assert not _same_assertion_source(fetched, candidate)


@pytest.mark.parametrize(
    ("candidate_time", "stored_time", "equivalent"),
    [
        (None, None, True),
        ("", None, True),
        ("   ", None, True),
        (None, datetime(2025, 6, 30, tzinfo=UTC), False),
        (" invalid-instant ", "invalid-instant", True),
        ("invalid-instant", None, False),
        ("invalid-instant", "other-invalid-instant", False),
    ],
    ids=[
        "missing",
        "empty",
        "blank",
        "present",
        "invalid-trimmed",
        "invalid-missing",
        "invalid-distinct",
    ],
)
def test_audit_optional_instants_preserve_missing_and_invalid_values(
    candidate_time: object, stored_time: object, equivalent: bool
) -> None:
    """Pure equality controls; invalid text is not claimed to be valid PostgreSQL input."""
    candidate = _row_values(
        {
            **_base_pg_row(fact_id="audit:optional-time", plan_id="AUDIT"),
            "valid_to": candidate_time,
        }
    )
    fetched = list(candidate)
    fetched[_STAGING_CORE_METRIC_COLUMNS.index("valid_to")] = stored_time
    assert _same_assertion(fetched, candidate) is equivalent
    # Validity changes content without changing the document/assertion source.
    assert _same_assertion_source(fetched, candidate)


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
    from pension_data.db.strategy import migration_file_paths

    migration = (
        Path(__file__).parents[2]
        / "src/pension_data/db/migrations/20260303_101_pg_core_fact_staging.sql"
    )
    assert migration in migration_file_paths(dialect="postgresql")

    class RecordingConnection:
        """Observe actual migration dispatch; no database semantics are simulated."""

        def __init__(self) -> None:
            self.statements: list[str] = []
            self.committed = False

        def execute(self, sql: str, params: tuple[object, ...] = ()) -> RecordingConnection:
            self.statements.append(sql)
            return self

        def fetchall(self) -> list[tuple[str]]:
            return []

        def commit(self) -> None:
            self.committed = True

        def rollback(self) -> None:
            raise AssertionError("migration dispatch unexpectedly rolled back")

    connection = RecordingConnection()
    report = apply_migrations(connection, dialect="postgresql")
    assert migration.stem in report.applied_versions
    assert connection.committed
    for statement in migration.read_text(encoding="utf-8").split(";"):
        if statement.strip():
            assert statement.strip() in connection.statements

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


@pytest.mark.parametrize(
    ("subject_identity", "peer_identity", "distinct_identity", "expected_security_id"),
    [
        pytest.param(
            ("037833100", "AAPL"),
            ("037833100", "OTHER"),
            ("594918104", "AAPL"),
            "cusip:037833100",
            id="cusip-precedes-ticker",
        ),
        pytest.param(
            (None, " aapl "),
            (None, "AAPL"),
            (None, "MSFT"),
            "ticker:AAPL",
            id="ticker-fallback",
        ),
    ],
)
@pytest.mark.parametrize(
    "subject_names",
    [("APPLE INC",), ("APPLE INC", "Apple Inc.")],
    ids=["single-subject-lot", "renamed-subject-lots"],
)
@pytest.mark.parametrize(
    "peer_names",
    [("Apple Incorporated",), ("Apple Incorporated", "Apple Corporation")],
    ids=["single-peer-lot", "renamed-peer-lots"],
)
def test_audit_security_identity_survives_names_and_separates_instruments(
    subject_names: tuple[str, ...],
    peer_names: tuple[str, ...],
    subject_identity: tuple[str | None, str],
    peer_identity: tuple[str | None, str],
    distinct_identity: tuple[str | None, str],
    expected_security_id: str,
) -> None:
    from pension_data.extract.investment.security_positions import (
        SecurityPositionInput,
        build_security_positions,
    )
    from pension_data.query.saved_views.holdings_ingest import to_holdings_overlap_inputs
    from pension_data.query.saved_views.service import execute_holdings_overlap_view

    def position(plan: str, identity: tuple[str | None, str], name: str, exposure: float = 10):
        return build_security_positions(
            plan_id=plan,
            plan_period="FY2024",
            rows=[
                SecurityPositionInput(
                    security_name=name,
                    cusip=identity[0],
                    ticker=identity[1],
                    shares=1,
                    market_value_usd=exposure,
                    asset_class="public_equity",
                    source="manual",
                    as_of="2024-06-30",
                    provenance_ref=f"doc:{plan}",
                )
            ],
        )[0]

    subjects = [
        position("A", subject_identity, name, 10 / len(subject_names)) for name in subject_names
    ]
    peers = [position("B", peer_identity, name, 15 / len(peer_names)) for name in peer_names]
    distinct = position("C", distinct_identity, "APPLE INC")
    adapted = to_holdings_overlap_inputs([*subjects, *peers, distinct])
    assert {row.manager_name for row in adapted[:-1]} == {expected_security_id}
    assert {row.fund_name for row in adapted[:-1]} == {expected_security_id}
    assert [row.display_name for row in adapted[:-1]] == [*subject_names, *peer_names]
    assert adapted[-1].manager_name != adapted[0].manager_name
    result = execute_holdings_overlap_view(adapted, subject_plan_id="A", plan_period="FY2024")
    shared = [row for row in result if row.counterparty_plan_id == "B"]
    # Both plans' renamed lots must aggregate before taking min(10, 15).
    assert len(shared) == 1 and shared[0].overlap_usd == 10
    separate = [row for row in result if row.counterparty_plan_id == "C"]
    assert len(separate) == 2
    assert all(row.overlap_status == "unknown_due_to_non_disclosure" for row in separate)


@pytest.mark.parametrize(
    ("peer_name", "equivalent"),
    [
        pytest.param("  apple\tINC  ", True, id="case-and-whitespace"),
        pytest.param("Apple Inc.", False, id="punctuation-distinct"),
        pytest.param("Apple Incorporated", False, id="spelling-distinct"),
    ],
)
def test_audit_name_only_security_identity_uses_its_own_contract(
    peer_name: str, equivalent: bool
) -> None:
    """Without CUSIP/ticker, names carry identity as well as display text."""
    from pension_data.extract.investment.security_positions import (
        SecurityPositionInput,
        build_security_positions,
    )
    from pension_data.query.saved_views.holdings_ingest import to_holdings_overlap_inputs
    from pension_data.query.saved_views.service import execute_holdings_overlap_view

    positions = []
    for plan, name, exposure in (("A", "APPLE INC", 10), ("B", peer_name, 15)):
        positions.extend(
            build_security_positions(
                plan_id=plan,
                plan_period="FY2024",
                rows=[
                    SecurityPositionInput(
                        security_name=name,
                        cusip=None,
                        ticker=None,
                        shares=1,
                        market_value_usd=exposure,
                        asset_class="public_equity",
                        source="manual",
                        as_of="2024-06-30",
                        provenance_ref=f"doc:{plan}",
                    )
                ],
            )
        )
    adapted = to_holdings_overlap_inputs(positions)
    assert adapted[0].manager_name == adapted[0].fund_name == "name:apple inc"
    assert (adapted[0].manager_name == adapted[1].manager_name) is equivalent
    assert (adapted[0].fund_name == adapted[1].fund_name) is equivalent
    # The builder trims display edges; the adapter preserves that producer text.
    assert [row.display_name for row in adapted] == ["APPLE INC", peer_name.strip()]

    result = execute_holdings_overlap_view(adapted, subject_plan_id="A", plan_period="FY2024")
    assert {row.counterparty_plan_id for row in result} == {"B"}
    if equivalent:
        assert len(result) == 1
        assert result[0].overlap_usd == 10
    else:
        assert len(result) == 2
        assert all(row.overlap_usd is None for row in result)
        assert all(row.overlap_status == "unknown_due_to_non_disclosure" for row in result)


@pytest.mark.parametrize(
    ("subject_cusip", "peer_cusip", "equivalent"),
    [
        pytest.param("037833100", "037833100", True, id="exact-cusip"),
        pytest.param("037833100", "037-833-100", False, id="punctuated-cusip"),
        pytest.param("03783310X", "03783310x", False, id="lowercase-cusip"),
    ],
)
def test_audit_direct_caller_unnormalized_cusip_remains_unknown_boundary(
    subject_cusip: str, peer_cusip: str, equivalent: bool
) -> None:
    """Pin the unknown direct-caller CUSIP contract for human decision (#964).

    Parsers normalize CUSIPs, but ``build_security_positions`` itself does not:
    a direct caller supplying punctuated/lowercase CUSIPs currently derives a
    different ``security_id`` than the canonical form, even under a different
    issuer display name. This characterizes current behavior through the real
    builder -> adapter -> overlap view; it does not decide that direct callers
    should (or should not) be normalized, and no repair is bundled here.
    """
    from pension_data.extract.investment.security_positions import (
        SecurityPositionInput,
        build_security_positions,
        load_own_holdings_csv,
    )
    from pension_data.query.saved_views.holdings_ingest import to_holdings_overlap_inputs
    from pension_data.query.saved_views.service import execute_holdings_overlap_view

    def direct(plan: str, cusip: str, name: str, exposure: float):
        return build_security_positions(
            plan_id=plan,
            plan_period="FY2024",
            rows=[
                SecurityPositionInput(
                    security_name=name,
                    cusip=cusip,
                    ticker=None,
                    shares=1,
                    market_value_usd=exposure,
                    asset_class="public_equity",
                    source="manual",
                    as_of="2024-06-30",
                    provenance_ref=f"doc:{plan}",
                )
            ],
        )[0]

    subject = direct("A", subject_cusip, "APPLE INC", 10)
    peer = direct("B", peer_cusip, "Apple Incorporated", 15)
    # The builder preserves the raw direct-caller CUSIP in identity and field.
    assert subject.security_id == f"cusip:{subject_cusip}"
    assert peer.security_id == f"cusip:{peer_cusip}"
    assert peer.cusip == peer_cusip

    adapted = to_holdings_overlap_inputs([subject, peer])
    assert (adapted[0].manager_name == adapted[1].manager_name) is equivalent
    assert (adapted[0].fund_name == adapted[1].fund_name) is equivalent
    assert [row.display_name for row in adapted] == ["APPLE INC", "Apple Incorporated"]

    result = execute_holdings_overlap_view(adapted, subject_plan_id="A", plan_period="FY2024")
    assert {row.counterparty_plan_id for row in result} == {"B"}
    if equivalent:
        assert len(result) == 1
        assert result[0].overlap_usd == 10
    else:
        assert len(result) == 2
        assert all(row.overlap_usd is None for row in result)
        assert all(row.overlap_status == "unknown_due_to_non_disclosure" for row in result)

    # Contrast: the parser producer normalizes the same peer CUSIP, so the
    # parsed path lands on the subject's canonical identity.
    parsed_peer = build_security_positions(
        plan_id="B",
        plan_period="FY2024",
        rows=load_own_holdings_csv(
            "security_name,cusip,ticker,shares,market_value_usd,asset_class\n"
            f"Apple Incorporated,{peer_cusip},,1,15,public_equity\n",
            as_of="2024-06-30",
            provenance_ref="doc:B",
        ),
    )[0]
    assert parsed_peer.security_id == subject.security_id
    parsed_result = execute_holdings_overlap_view(
        to_holdings_overlap_inputs([subject, parsed_peer]),
        subject_plan_id="A",
        plan_period="FY2024",
    )
    assert len(parsed_result) == 1
    assert parsed_result[0].overlap_usd == 10


@pytest.mark.parametrize(
    "declaration", ["TIMESTAMPTZ ARRAY", "TIMESTAMPTZ ARRAY[3]", "TIMESTAMPTZ []"]
)
def test_audit_schema_rejects_array_type_suffixes(declaration: str) -> None:
    assert _declared_sql_type(f"  valid_to {declaration},", "valid_to") != "TIMESTAMPTZ"
