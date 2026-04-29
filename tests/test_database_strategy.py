"""Compatibility smoke tests for DB strategy migration expectations.

These assertions intentionally mirror PR acceptance criteria paths.
"""

from __future__ import annotations

from pension_data.db.strategy import bootstrap_database_connection


def test_expected_table_list_includes_staging_consultant_engagements() -> None:
    _config, connection = bootstrap_database_connection(
        environment="local",
        database_url="sqlite:///:memory:",
        apply_migrations_on_boot=True,
    )
    try:
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()
        table_names = {row[0] for row in rows}
    finally:
        connection.close()

    assert "staging_consultant_engagements" in table_names


def test_migrations_create_staging_consultant_engagements_table() -> None:
    _config, connection = bootstrap_database_connection(
        environment="local",
        database_url="sqlite:///:memory:",
        apply_migrations_on_boot=True,
    )
    try:
        row = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
            ("staging_consultant_engagements",),
        ).fetchone()
    finally:
        connection.close()

    assert row == ("staging_consultant_engagements",)
