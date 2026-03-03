"""Database strategy resolution and connection helpers."""

from __future__ import annotations

import importlib
import importlib.util
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

DatabaseDialect = Literal["sqlite", "postgresql"]
DatabaseEnvironment = Literal["local", "production"]

DEFAULT_LOCAL_SQLITE_URL = "sqlite:///./.pension-data/pension_data.db"


@dataclass(frozen=True, slots=True)
class DatabaseConfig:
    """Resolved database strategy configuration."""

    environment: DatabaseEnvironment
    dialect: DatabaseDialect
    database_url: str


def _normalize_database_url(database_url: str) -> str:
    normalized = database_url.strip()
    if not normalized:
        raise ValueError("database_url must be non-empty")
    if normalized.startswith("postgres://"):
        return "postgresql://" + normalized[len("postgres://") :]
    return normalized


def _dialect_for_url(database_url: str) -> DatabaseDialect:
    if database_url.startswith("sqlite:///"):
        return "sqlite"
    if database_url.startswith("postgresql://"):
        return "postgresql"
    raise ValueError(
        "database_url must start with sqlite:/// or postgresql:// " f"(received: {database_url})"
    )


def resolve_database_config(
    *,
    environment: DatabaseEnvironment = "local",
    database_url: str | None = None,
) -> DatabaseConfig:
    """Resolve dialect and connection URL for local or production execution."""
    resolved_url = _normalize_database_url(database_url or DEFAULT_LOCAL_SQLITE_URL)
    dialect = _dialect_for_url(resolved_url)
    if environment == "production" and dialect != "postgresql":
        raise ValueError("production environment requires a postgresql:// database_url")
    return DatabaseConfig(
        environment=environment,
        dialect=dialect,
        database_url=resolved_url,
    )


def migration_file_paths(*, dialect: DatabaseDialect) -> tuple[Path, ...]:
    """Return deterministic migration sequence for the selected dialect."""
    migrations_root = Path(__file__).resolve().parent / "migrations"
    if dialect == "sqlite":
        return (
            migrations_root / "20260302_001_core_fact_staging.sql",
            migrations_root / "20260302_002_seed_backfill_compat.sql",
        )
    return (
        migrations_root / "20260303_101_pg_core_fact_staging.sql",
        migrations_root / "20260303_102_pg_seed_backfill_compat.sql",
    )


def connect_database(config: DatabaseConfig) -> Any:
    """Open a DB-API connection for the configured dialect."""
    if config.dialect == "sqlite":
        path_token = config.database_url[len("sqlite:///") :]
        sqlite_target = ":memory:" if path_token == ":memory:" else path_token
        if sqlite_target != ":memory:":
            path = Path(sqlite_target).expanduser()
            if not path.is_absolute():
                path = Path.cwd() / path
            path.parent.mkdir(parents=True, exist_ok=True)
            sqlite_target = str(path)
        return sqlite3.connect(sqlite_target)

    psycopg_spec = importlib.util.find_spec("psycopg")
    if psycopg_spec is None:
        raise RuntimeError(
            "PostgreSQL support requires psycopg. Install with: pip install -e '.[postgres]'"
        )
    psycopg = importlib.import_module("psycopg")
    return psycopg.connect(config.database_url)


def database_setup_requirements(config: DatabaseConfig) -> tuple[str, ...]:
    """Return concise setup requirements for the selected DB strategy."""
    if config.dialect == "sqlite":
        return (
            "No external DB server required.",
            "SQLite file is created automatically on first connection.",
            "Use sqlite:///:memory: for isolated test runs.",
        )
    return (
        "Provision PostgreSQL 15+ for shared/production workloads.",
        "Install psycopg client dependency in runtime environment.",
        "Apply PostgreSQL migration sequence before enabling query endpoints.",
    )
