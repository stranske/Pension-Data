#!/usr/bin/env python3
"""CLI utility to apply pension-data DB migrations."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict

from pension_data.db.migrations_runner import run_migrations_for_config
from pension_data.db.strategy import resolve_database_config


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply dialect-aware DB migrations.")
    parser.add_argument(
        "--environment",
        choices=["local", "production"],
        default="local",
        help="Strategy environment (local uses SQLite by default).",
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="Optional DB URL override (sqlite:///... or postgresql://...).",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    config = resolve_database_config(
        environment=args.environment,
        database_url=args.database_url,
    )
    report = run_migrations_for_config(config)
    payload = {
        "environment": config.environment,
        "dialect": config.dialect,
        "database_url": config.database_url,
        "report": asdict(report),
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
