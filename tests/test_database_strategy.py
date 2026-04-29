"""Compatibility smoke tests for DB strategy migration expectations.

These assertions intentionally mirror PR acceptance criteria paths.
"""

from __future__ import annotations

from pension_data.db.strategy import bootstrap_database_connection, migration_file_paths


def test_consultant_engagements_table_must_remain_for_migration_dependency() -> None:
    sqlite_paths = migration_file_paths(dialect="sqlite")
    postgres_paths = migration_file_paths(dialect="postgresql")

    sqlite_core_sql = sqlite_paths[0].read_text(encoding="utf-8")
    sqlite_extended_sql = sqlite_paths[2].read_text(encoding="utf-8")
    postgres_core_sql = postgres_paths[0].read_text(encoding="utf-8")
    postgres_extended_sql = postgres_paths[2].read_text(encoding="utf-8")

    table_ddl = "CREATE TABLE IF NOT EXISTS staging_consultant_engagements"
    dependent_index = (
        "CREATE INDEX IF NOT EXISTS idx_consultant_engagements_plan "
        "ON staging_consultant_engagements(plan_id, plan_period);"
    )

    assert table_ddl in sqlite_core_sql
    assert dependent_index in sqlite_extended_sql
    assert table_ddl in postgres_core_sql
    assert dependent_index in postgres_extended_sql


def test_consultant_engagements_migration_remains_in_order_for_both_dialects() -> None:
    sqlite_paths = migration_file_paths(dialect="sqlite")
    postgres_paths = migration_file_paths(dialect="postgresql")

    assert sqlite_paths[0].name == "20260302_001_core_fact_staging.sql"
    assert sqlite_paths[2].name == "20260307_003_extended_staging.sql"
    assert postgres_paths[0].name == "20260303_101_pg_core_fact_staging.sql"
    assert postgres_paths[2].name == "20260307_103_pg_extended_staging.sql"

    table_ddl = "CREATE TABLE IF NOT EXISTS staging_consultant_engagements"
    dependent_index = "idx_consultant_engagements_plan"

    assert table_ddl in sqlite_paths[0].read_text(encoding="utf-8")
    assert table_ddl in postgres_paths[0].read_text(encoding="utf-8")
    assert dependent_index in sqlite_paths[2].read_text(encoding="utf-8")
    assert dependent_index in postgres_paths[2].read_text(encoding="utf-8")


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


def test_migrations_create_consultant_engagements_index_dependency() -> None:
    _config, connection = bootstrap_database_connection(
        environment="local",
        database_url="sqlite:///:memory:",
        apply_migrations_on_boot=True,
    )
    try:
        row = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?",
            ("idx_consultant_engagements_plan",),
        ).fetchone()
    finally:
        connection.close()

    assert row == ("idx_consultant_engagements_plan",)


def test_consultant_engagements_table_precedes_dependent_index_for_each_dialect() -> None:
    sqlite_paths = migration_file_paths(dialect="sqlite")
    postgres_paths = migration_file_paths(dialect="postgresql")

    sqlite_core_sql = sqlite_paths[0].read_text(encoding="utf-8")
    sqlite_extended_sql = sqlite_paths[2].read_text(encoding="utf-8")
    postgres_core_sql = postgres_paths[0].read_text(encoding="utf-8")
    postgres_extended_sql = postgres_paths[2].read_text(encoding="utf-8")

    create_table = "CREATE TABLE IF NOT EXISTS staging_consultant_engagements"
    create_index = "CREATE INDEX IF NOT EXISTS idx_consultant_engagements_plan"

    assert create_table in sqlite_core_sql
    assert create_table not in sqlite_extended_sql
    assert create_index not in sqlite_core_sql
    assert create_index in sqlite_extended_sql

    assert create_table in postgres_core_sql
    assert create_table not in postgres_extended_sql
    assert create_index not in postgres_core_sql
    assert create_index in postgres_extended_sql


def test_sqlite_migration_history_keeps_core_before_extended_consultant_dependencies() -> None:
    _config, connection = bootstrap_database_connection(
        environment="local",
        database_url="sqlite:///:memory:",
        apply_migrations_on_boot=True,
    )
    try:
        rows = connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall()
    finally:
        connection.close()

    versions = [row[0] for row in rows]
    core_version = "20260302_001_core_fact_staging"
    extended_version = "20260307_003_extended_staging"

    assert core_version in versions
    assert extended_version in versions
    assert versions.index(core_version) < versions.index(extended_version)
