"""Shared test fixtures for the pension-data test suite."""

from __future__ import annotations

import sqlite3
from collections.abc import Generator
from pathlib import Path

import pytest

from pension_data.db.strategy import bootstrap_database_connection
from pension_data.query.run_record import default_run_record_root


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


def _checkout_artifact_sizes() -> dict[Path, int]:
    """Return every file currently under the checkout's default artifact root.

    Resolved from ``default_run_record_root()`` rather than a literal path so the
    guard below watches exactly the directory the recorders write to; a change to
    the production default moves both at once.
    """
    root = default_run_record_root()
    return {path: path.stat().st_size for path in root.rglob("*") if path.is_file()}


@pytest.fixture(scope="session", autouse=True)
def _no_run_artifacts_written_into_checkout() -> Generator[None, None, None]:
    """Fail the session if a test wrote a run artifact into the working tree.

    ``persist_nl_query_run_record`` and ``_persist_sql_query_run_record`` fall
    back to ``default_run_record_root()`` -- a repo-relative ``artifacts/`` path
    -- whenever a caller passes no ``run_record_root``. A test that omits it
    drops JSON into the checkout, where autofix's ``git add -A`` commits it: 26
    such files reached ``main`` that way before this guard existed. Tests must
    pass a ``tmp_path``-derived root (and ``log_path``) to any endpoint that
    records a run.

    Session-scoped on purpose: a per-test snapshot would misattribute a leak to
    whichever test happened to straddle it under ``pytest-xdist``.
    """
    before = _checkout_artifact_sizes()
    yield
    after = _checkout_artifact_sizes()
    leaked = sorted(str(path) for path, size in after.items() if before.get(path) != size)
    assert not leaked, (
        "the test suite wrote run artifacts into the checkout instead of tmp_path; "
        "pass run_record_root=/log_path= to the endpoint under test:\n  " + "\n  ".join(leaked)
    )
