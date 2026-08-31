"""Shared test fixtures for the pension-data test suite."""

from __future__ import annotations

import sqlite3
from collections.abc import Generator
from pathlib import Path
from typing import get_args

import pytest

from pension_data.db.strategy import bootstrap_database_connection
from pension_data.langchain.observability import default_nl_log_path
from pension_data.query.run_record import RunSurface, _surface_dir, default_run_record_root


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


def _run_artifact_watch_paths() -> tuple[Path, ...]:
    """Return the checkout paths the query run recorders write to by default.

    Derived from ``_surface_dir`` over every ``RunSurface`` rather than spelled
    out, so a new surface extends the guard below without anyone remembering to,
    and a change to the production layout moves watcher and writer together.

    Deliberately narrower than the whole ``artifacts/`` root: under GitHub
    Actions, ``_write_ci_langsmith_fleet_artifact`` in
    ``tests/observability/test_langsmith_fleet.py`` writes
    ``artifacts/langsmith/langsmith-fleet.ndjson`` into the checkout on purpose,
    for the workflow's upload-artifact step to collect. Watching the whole root
    fails CI on that intentional write. Do not widen this to ``artifacts/``.
    """
    root = default_run_record_root()
    return (
        *(root / _surface_dir(surface) for surface in get_args(RunSurface)),
        default_nl_log_path(),
    )


def _run_artifact_sizes() -> dict[Path, int]:
    """Map each existing watched run artifact to its current size."""
    sizes: dict[Path, int] = {}
    for target in _run_artifact_watch_paths():
        if target.is_file():
            sizes[target] = target.stat().st_size
            continue
        sizes.update({path: path.stat().st_size for path in target.rglob("*") if path.is_file()})
    return sizes


@pytest.fixture(scope="session", autouse=True)
def _no_run_artifacts_written_into_checkout() -> Generator[None, None, None]:
    """Fail the session if a test wrote a run artifact into the working tree.

    ``persist_nl_query_run_record`` and ``_persist_sql_query_run_record`` fall
    back to ``default_run_record_root()`` -- a repo-relative ``artifacts/`` path
    -- whenever a caller passes no ``run_record_root``, and the NL operation log
    falls back to ``default_nl_log_path()`` the same way. A test that omits
    either drops files into the checkout, where autofix's ``git add -A`` commits
    them: 26 such files reached ``main`` that way before this guard existed, and
    three more test leaks went unnoticed for longer still because the log path
    they wrote to is gitignored. Tests must pass a ``tmp_path``-derived
    ``run_record_root`` and ``log_path`` to any endpoint that records a run.

    Session-scoped on purpose: a per-test snapshot would misattribute a leak to
    whichever test happened to straddle it under ``pytest-xdist``. Each xdist
    worker is its own session, so a real leak is reported once per worker.
    """
    before = _run_artifact_sizes()
    yield
    after = _run_artifact_sizes()
    leaked = sorted(str(path) for path, size in after.items() if before.get(path) != size)
    assert not leaked, (
        "the test suite wrote run artifacts into the checkout instead of tmp_path; "
        "pass run_record_root=/log_path= to the endpoint under test:\n  " + "\n  ".join(leaked)
    )
