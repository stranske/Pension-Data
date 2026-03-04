#!/usr/bin/env python3
"""Print lightweight failure/latency summary for NL operation logs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pension_data.langchain.observability import (
    default_nl_log_path,
    load_nl_operation_logs,
    summarize_nl_operation_logs,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--log-path",
        default=str(default_nl_log_path()),
        help="Path to NL operation JSONL log",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of most recent rows to include",
    )
    args = parser.parse_args()

    entries = load_nl_operation_logs(Path(args.log_path), limit=args.limit)
    summary = summarize_nl_operation_logs(entries)
    print(json.dumps(summary.__dict__, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
