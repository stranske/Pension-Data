"""Shared test fixtures for the pension-data test suite."""

from __future__ import annotations

import sqlite3
from collections.abc import Generator

import pytest

from pension_data.db.strategy import bootstrap_database_connection


@pytest.fixture()
def in_memory_db() -> Generator[sqlite3.Connection, None, None]:
    """Create an in-memory SQLite connection with all migrations applied."""
    _config, connection = bootstrap_database_connection(
        environment="local",
        database_url="sqlite:///:memory:",
        apply_migrations_on_boot=True,
    )
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture()
def sample_plan_id() -> str:
    return "CA-PERS"


@pytest.fixture()
def sample_plan_period() -> str:
    return "FY2024"
