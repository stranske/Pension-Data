"""Tests for SQLite-local/PostgreSQL-production DB strategy helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from pension_data.db.strategy import (
    DEFAULT_LOCAL_SQLITE_URL,
    bootstrap_database_connection,
    connect_database,
    database_setup_requirements,
    migration_file_paths,
    resolve_database_config,
)


def test_local_default_config_uses_sqlite_without_server_requirement() -> None:
    config = resolve_database_config()

    assert config.environment == "local"
    assert config.dialect == "sqlite"
    assert config.database_url == DEFAULT_LOCAL_SQLITE_URL
    requirements = database_setup_requirements(config)
    assert any("No external DB server required." in item for item in requirements)


def test_production_config_requires_postgresql_url() -> None:
    with pytest.raises(ValueError, match="production environment requires a postgresql://"):
        resolve_database_config(environment="production", database_url="sqlite:///:memory:")

    config = resolve_database_config(
        environment="production",
        database_url="postgres://user:pass@localhost:5432/pension_data",
    )
    assert config.dialect == "postgresql"
    assert config.database_url.startswith("postgresql://")


def test_migration_paths_are_present_for_sqlite_and_postgresql_sequences() -> None:
    sqlite_paths = migration_file_paths(dialect="sqlite")
    postgres_paths = migration_file_paths(dialect="postgresql")

    assert len(sqlite_paths) == 3
    assert len(postgres_paths) == 3
    assert all(path.exists() for path in sqlite_paths)
    assert all(path.exists() for path in postgres_paths)
    assert "pg_core_fact_staging" in postgres_paths[0].name
    assert "extended_staging" in sqlite_paths[2].name
    assert "pg_extended_staging" in postgres_paths[2].name


def test_sqlite_connection_path_is_created_and_roundtrips_queries(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "local" / "pension_data.db"
    config = resolve_database_config(database_url=f"sqlite:///{sqlite_path}")
    connection = connect_database(config)
    try:
        connection.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        connection.execute("INSERT INTO sample (id, value) VALUES (?, ?)", (1, "alpha"))
        row = connection.execute("SELECT value FROM sample WHERE id = 1").fetchone()
        assert row == ("alpha",)
    finally:
        connection.close()

    assert sqlite_path.exists()


def test_postgresql_connection_without_driver_raises_helpful_runtime_error() -> None:
    config = resolve_database_config(
        environment="production",
        database_url="postgresql://user:pass@localhost:5432/pension_data",
    )
    try:
        import psycopg  # type: ignore  # noqa: F401
    except ImportError:
        with pytest.raises(RuntimeError, match="requires psycopg"):
            connect_database(config)
    else:
        pytest.skip("psycopg is installed; runtime error path is not expected")


def test_bootstrap_connection_applies_sqlite_migrations() -> None:
    config, connection = bootstrap_database_connection(
        environment="local",
        database_url="sqlite:///:memory:",
        apply_migrations_on_boot=True,
    )
    try:
        row = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'"
        ).fetchone()
    finally:
        connection.close()

    assert config.dialect == "sqlite"
    assert row == ("schema_migrations",)


def test_extended_migration_creates_all_domain_tables() -> None:
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

    expected_tables = {
        "schema_migrations",
        # Phase-1 core tables
        "staging_core_metrics",
        "staging_cash_flows",
        "staging_manager_fund_vehicle_relationships",
        "staging_consultant_engagements",
        # Extended domain tables
        "staging_funded_actuarial",
        "staging_allocations",
        "staging_manager_fees",
        "staging_positions",
        "staging_risk_exposures",
        "staging_manager_lifecycle",
        "staging_financial_flows",
        "staging_consultant_attribution",
        "registry_systems",
        "canonical_entities",
        "entity_source_links",
        "entity_lineage_events",
        "api_keys",
        "raw_artifacts",
        "evidence_references",
        "metric_evidence_links",
        "extraction_review_queue",
        "entity_review_queue",
        "discovered_inventory",
    }
    assert expected_tables.issubset(table_names), f"Missing tables: {expected_tables - table_names}"


def test_expected_domain_table_list_includes_staging_consultant_engagements() -> None:
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


def test_migration_path_creates_staging_consultant_engagements_table() -> None:
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
