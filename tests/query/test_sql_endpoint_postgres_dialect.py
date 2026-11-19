"""Tests for PostgreSQL dialect behavior in read-only SQL execution service."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from pension_data.query.sql_service import SQLQueryRequest, execute_sql_query


@dataclass(slots=True)
class _FakeCursor:
    rows: list[tuple[Any, ...]]
    description: tuple[tuple[str], ...] | None = None

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


class _FakePostgresConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Mapping[str, Any] | tuple[Any, ...] | tuple[()]]] = []

    def execute(
        self,
        sql: str,
        params: Mapping[str, Any] | tuple[Any, ...] | tuple[()] = (),
    ) -> _FakeCursor:
        self.calls.append((sql, params))
        if "COUNT(*) AS _pd_total_rows" in sql:
            return _FakeCursor(rows=[(3,)], description=(("_pd_total_rows",),))
        return _FakeCursor(
            rows=[(1, "m-001"), (2, "m-002")],
            description=(("id",), ("metric",)),
        )


class _TimeoutPostgresConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Mapping[str, Any] | tuple[Any, ...] | tuple[()]]] = []

    def execute(
        self,
        sql: str,
        params: Mapping[str, Any] | tuple[Any, ...] | tuple[()] = (),
    ) -> _FakeCursor:
        self.calls.append((sql, params))
        if "COUNT(*) AS _pd_total_rows" in sql:
            raise RuntimeError("canceling statement due to statement timeout")
        return _FakeCursor(rows=[])


def test_postgresql_dialect_uses_named_limit_offset_placeholders() -> None:
    connection = _FakePostgresConnection()
    response = execute_sql_query(
        connection=connection,
        request=SQLQueryRequest(
            sql="SELECT id, metric FROM sample_metrics WHERE id >= %(min_id)s ORDER BY id",
            params={"min_id": 1},
            page=1,
            page_size=2,
            max_rows=10,
        ),
        caller_key_id="key:test",
        dialect="postgresql",
    )

    assert response.status == "ok"
    assert response.rows == ((1, "m-001"), (2, "m-002"))
    assert len(connection.calls) == 4
    assert connection.calls[0] == ("SET statement_timeout = 2000", ())
    assert "LIMIT %(_pd_limit)s OFFSET %(_pd_offset)s" in connection.calls[2][0]
    assert connection.calls[2][1]["_pd_limit"] == 2
    assert connection.calls[2][1]["_pd_offset"] == 0
    assert connection.calls[3] == ("SET statement_timeout = DEFAULT", ())


def test_postgresql_dialect_uses_positional_limit_offset_placeholders() -> None:
    connection = _FakePostgresConnection()
    response = execute_sql_query(
        connection=connection,
        request=SQLQueryRequest(
            sql="SELECT id, metric FROM sample_metrics WHERE id >= %s ORDER BY id",
            params=(1,),
            page=2,
            page_size=1,
            max_rows=10,
        ),
        caller_key_id="key:test",
        dialect="postgresql",
    )

    assert response.status == "ok"
    assert len(connection.calls) == 4
    assert connection.calls[0] == ("SET statement_timeout = 2000", ())
    assert "LIMIT %s OFFSET %s" in connection.calls[2][0]
    assert connection.calls[2][1] == (1, 1, 1)
    assert connection.calls[3] == ("SET statement_timeout = DEFAULT", ())


def test_postgresql_statement_timeout_maps_to_timeout_error() -> None:
    connection = _TimeoutPostgresConnection()
    response = execute_sql_query(
        connection=connection,
        request=SQLQueryRequest(
            sql="SELECT id FROM sample_metrics",
            page=1,
            page_size=10,
            max_rows=100,
        ),
        caller_key_id="key:test",
        dialect="postgresql",
    )

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "TIMEOUT"
    assert connection.calls[0] == ("SET statement_timeout = 2000", ())
    assert connection.calls[-1] == ("SET statement_timeout = DEFAULT", ())


class _AbortOnResetPostgresConnection:
    """Simulates Postgres leaving the transaction aborted after a failed reset."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any] | tuple[str, ...]] = []
        self._reset_attempts = 0

    def execute(
        self,
        sql: str,
        params: Mapping[str, Any] | tuple[Any, ...] | tuple[()] = (),
    ) -> _FakeCursor:
        self.calls.append((sql, params))
        if sql == "SET statement_timeout = DEFAULT":
            self._reset_attempts += 1
            if self._reset_attempts == 1:
                raise RuntimeError("current transaction is aborted")
            return _FakeCursor(rows=[])
        if "COUNT(*) AS _pd_total_rows" in sql:
            return _FakeCursor(rows=[(2,)], description=(("_pd_total_rows",),))
        return _FakeCursor(rows=[(1,)], description=(("id",),))

    def rollback(self) -> None:
        self.calls.append(("rollback", ()))


def test_postgresql_clear_timeout_rolls_back_aborted_transaction() -> None:
    connection = _AbortOnResetPostgresConnection()
    response = execute_sql_query(
        connection=connection,
        request=SQLQueryRequest(
            sql="SELECT id FROM sample_metrics",
            page=1,
            page_size=1,
            max_rows=10,
        ),
        caller_key_id="key:test",
        dialect="postgresql",
    )

    assert response.status == "ok"
    assert response.rows == ((1,),)
    assert connection.calls[-3:] == [
        ("SET statement_timeout = DEFAULT", ()),
        ("rollback", ()),
        ("SET statement_timeout = DEFAULT", ()),
    ]


class _NoRollbackPostgresConnection:
    """Postgres stub without rollback when statement_timeout reset fails."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any] | tuple[str, ...]] = []

    def execute(
        self,
        sql: str,
        params: Mapping[str, Any] | tuple[Any, ...] | tuple[()] = (),
    ) -> _FakeCursor:
        self.calls.append((sql, params))
        if sql == "SET statement_timeout = DEFAULT":
            raise RuntimeError("current transaction is aborted")
        if "COUNT(*) AS _pd_total_rows" in sql:
            return _FakeCursor(rows=[(1,)], description=(("_pd_total_rows",),))
        return _FakeCursor(rows=[(1,)], description=(("id",),))


def test_postgresql_clear_timeout_without_rollback_swallows_reset_failure() -> None:
    connection = _NoRollbackPostgresConnection()
    response = execute_sql_query(
        connection=connection,
        request=SQLQueryRequest(
            sql="SELECT id FROM sample_metrics",
            page=1,
            page_size=1,
            max_rows=10,
        ),
        caller_key_id="key:test",
        dialect="postgresql",
    )

    assert response.status == "ok"
    reset_calls = [
        call for call in connection.calls if call[0] == "SET statement_timeout = DEFAULT"
    ]
    assert len(reset_calls) == 1
