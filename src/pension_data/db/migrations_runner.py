"""Dialect-aware migration runner with idempotent state tracking."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pension_data.db.strategy import (
    DatabaseConfig,
    DatabaseDialect,
    connect_database,
    migration_file_paths,
)

_MIGRATION_STATE_TABLE = "schema_migrations"


@dataclass(frozen=True, slots=True)
class MigrationRunReport:
    """Deterministic migration execution report."""

    dialect: DatabaseDialect
    migration_count: int
    applied_versions: tuple[str, ...]
    skipped_versions: tuple[str, ...]


def _migration_version(path: Path) -> str:
    return path.stem


def _split_sql_statements(sql: str) -> tuple[str, ...]:
    statements = [statement.strip() for statement in sql.split(";")]
    return tuple(statement for statement in statements if statement)


def _execute(connection: Any, sql: str, params: tuple[object, ...] = ()) -> Any:
    return connection.execute(sql, params)


def _ensure_migration_state_table(connection: Any, *, dialect: DatabaseDialect) -> None:
    if dialect == "postgresql":
        _execute(
            connection,
            f"""
            CREATE TABLE IF NOT EXISTS {_MIGRATION_STATE_TABLE} (
              version TEXT PRIMARY KEY,
              applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """.strip(),
        )
        return
    _execute(
        connection,
        f"""
        CREATE TABLE IF NOT EXISTS {_MIGRATION_STATE_TABLE} (
          version TEXT PRIMARY KEY,
          applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """.strip(),
    )


def applied_migration_versions(connection: Any) -> tuple[str, ...]:
    cursor = _execute(
        connection,
        f"SELECT version FROM {_MIGRATION_STATE_TABLE} ORDER BY version",
    )
    rows = cursor.fetchall()
    return tuple(str(row[0]) for row in rows)


def _apply_sql_file(connection: Any, *, dialect: DatabaseDialect, sql: str) -> None:
    if dialect == "sqlite" and hasattr(connection, "executescript"):
        connection.executescript(sql)
        return

    for statement in _split_sql_statements(sql):
        _execute(connection, statement)


def apply_migrations(
    connection: Any,
    *,
    dialect: DatabaseDialect,
    paths: Sequence[Path] | None = None,
) -> MigrationRunReport:
    """Apply pending migrations in order; reruns are idempotent."""
    migration_paths = tuple(paths or migration_file_paths(dialect=dialect))
    try:
        _ensure_migration_state_table(connection, dialect=dialect)
        applied_versions = set(applied_migration_versions(connection))
        applied_now: list[str] = []
        skipped: list[str] = []
        for path in migration_paths:
            version = _migration_version(path)
            if version in applied_versions:
                skipped.append(version)
                continue
            sql = path.read_text(encoding="utf-8")
            _apply_sql_file(connection, dialect=dialect, sql=sql)
            if dialect == "postgresql":
                _execute(
                    connection,
                    f"INSERT INTO {_MIGRATION_STATE_TABLE} (version) VALUES (%s)",
                    (version,),
                )
            else:
                _execute(
                    connection,
                    f"INSERT INTO {_MIGRATION_STATE_TABLE} (version) VALUES (?)",
                    (version,),
                )
            applied_versions.add(version)
            applied_now.append(version)
        connection.commit()
    except Exception:  # noqa: BLE001
        connection.rollback()
        raise

    return MigrationRunReport(
        dialect=dialect,
        migration_count=len(migration_paths),
        applied_versions=tuple(applied_now),
        skipped_versions=tuple(skipped),
    )


def run_migrations_for_config(config: DatabaseConfig) -> MigrationRunReport:
    """Open the configured DB connection, apply migrations, and close safely."""
    connection = connect_database(config)
    try:
        return apply_migrations(connection, dialect=config.dialect)
    finally:
        connection.close()
