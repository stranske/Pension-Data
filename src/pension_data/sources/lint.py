"""CLI linter entrypoint for deterministic source-map validation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pension_data.sources.validate import load_source_map, validate_source_map_entries


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Lint pension-data source map CSV files.")
    parser.add_argument(
        "source_map",
        nargs="?",
        default="config/sources/source_map_v1.csv",
        help="Path to source-map CSV file.",
    )
    args = parser.parse_args(argv)

    try:
        entries = load_source_map(Path(args.source_map))
    except (FileNotFoundError, OSError, ValueError, TypeError, AttributeError) as exc:
        print(f"ERROR: {exc}")
        return 1

    findings = validate_source_map_entries(entries)
    if findings:
        for finding in findings:
            print(f"[{finding.code}] {finding.plan_id}: {finding.message}")
        return 1

    print(f"OK: {args.source_map} ({len(entries)} entries)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
