"""Real PostgreSQL integration tests for SQL endpoint pagination + timeout semantics."""

from __future__ import annotations

import os
from typing import Any

import pytest

from pension_data.db.migrations_runner import apply_migrations
from pension_data.db.strategy import connect_database, resolve_database_config
from pension_data.query.sql_service import SQLQueryRequest, execute_sql_query

POSTGRES_TEST_URL_ENV = "PENSION_DATA_TEST_POSTGRES_URL"


def _postgres_connection() -> Any:
    database_url = os.getenv(POSTGRES_TEST_URL_ENV)
    if not database_url:
        pytest.skip(f"{POSTGRES_TEST_URL_ENV} is not set")
    config = resolve_database_config(environment="production", database_url=database_url)
    connection = connect_database(config)
    apply_migrations(connection, dialect=config.dialect)
    return connection


def test_postgres_sql_query_paginates_against_real_connection() -> None:
    connection = _postgres_connection()
    try:
        connection.execute("DROP TABLE IF EXISTS sample_metrics")
        connection.execute("""
            CREATE TABLE sample_metrics (
              id INTEGER PRIMARY KEY,
              metric TEXT NOT NULL,
              value DOUBLE PRECISION NOT NULL
            )
            """)
        connection.executemany(
            "INSERT INTO sample_metrics (id, metric, value) VALUES (%s, %s, %s)",
            [(1, "m-001", 1.0), (2, "m-002", 2.0), (3, "m-003", 3.0), (4, "m-004", 4.0)],
        )
        connection.commit()

        response = execute_sql_query(
            connection=connection,
            request=SQLQueryRequest(
                sql="SELECT id, metric, value FROM sample_metrics ORDER BY id",
                page=2,
                page_size=2,
                max_rows=100,
            ),
            caller_key_id="postgres-integration",
            dialect="postgresql",
        )
    finally:
        connection.close()

    assert response.status == "ok"
    assert response.metadata.total_rows == 4
    assert response.rows == ((3, "m-003", 3.0), (4, "m-004", 4.0))


def test_postgres_sql_query_maps_statement_timeout_to_timeout_error() -> None:
    connection = _postgres_connection()
    try:
        response = execute_sql_query(
            connection=connection,
            request=SQLQueryRequest(
                sql="SELECT pg_sleep(0.05)",
                page=1,
                page_size=1,
                timeout_ms=1,
                max_rows=10,
            ),
            caller_key_id="postgres-integration",
            dialect="postgresql",
        )
    finally:
        connection.close()

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "TIMEOUT"
