"""Tests for migration runner idempotency and state tracking."""

from __future__ import annotations

from pension_data.db.migrations_runner import applied_migration_versions, apply_migrations
from pension_data.db.strategy import connect_database, resolve_database_config


def test_sqlite_migrations_apply_and_record_versions_idempotently() -> None:
    config = resolve_database_config(database_url="sqlite:///:memory:")
    connection = connect_database(config)
    try:
        first = apply_migrations(connection, dialect="sqlite")
        second = apply_migrations(connection, dialect="sqlite")
        versions = applied_migration_versions(connection)
    finally:
        connection.close()

    assert len(first.applied_versions) == 2
    assert first.skipped_versions == ()
    assert second.applied_versions == ()
    assert len(second.skipped_versions) == 2
    assert versions == tuple(sorted(first.applied_versions))
