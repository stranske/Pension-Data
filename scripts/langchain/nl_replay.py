#!/usr/bin/env python3
"""Replay one logged NL request deterministically against a SQLite database."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from pension_data.langchain.observability import (
    NLOperationLogEntry,
    default_nl_log_path,
    load_nl_operation_logs,
    replay_logged_request,
)


def _select_request(log_path: Path, request_id: str | None) -> NLOperationLogEntry:
    entries = load_nl_operation_logs(log_path)
    if not entries:
        raise ValueError(f"no NL log entries found at {log_path}")
    if request_id is None:
        return entries[-1]
    for entry in reversed(entries):
        if entry.request_id == request_id:
            return entry
    raise ValueError(f"request_id '{request_id}' not found in {log_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", required=True, help="Path to SQLite database file")
    parser.add_argument(
        "--log-path",
        default=str(default_nl_log_path()),
        help="Path to NL operation JSONL log",
    )
    parser.add_argument("--request-id", help="Specific request_id to replay (default: latest)")
    args = parser.parse_args()

    log_path = Path(args.log_path)
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise ValueError(f"database path does not exist: {db_path}")

    entry = _select_request(log_path, args.request_id)
    connection = sqlite3.connect(db_path)
    try:
        response = replay_logged_request(entry=entry, connection=connection)
    finally:
        connection.close()

    payload = {
        "request_id": response.metadata.request_id,
        "status": response.status,
        "sql": response.sql,
        "returned_rows": response.metadata.returned_rows,
        "error_code": response.error.code if response.error is not None else None,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
