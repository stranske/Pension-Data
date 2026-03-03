"""CLI runner for fixture-driven entity regression checks."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in (None, ""):  # pragma: no cover - direct script execution support
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from tools.entity_regression.harness import load_fixture, run_entity_regression, write_report


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fixture",
        type=Path,
        required=True,
        help="Path to entity regression fixture JSON",
    )
    parser.add_argument(
        "--report-out",
        type=Path,
        default=Path("artifacts/entity_regression/report.json"),
        help="Path to write machine-readable entity regression report",
    )
    parser.add_argument(
        "--max-regressions",
        type=int,
        default=0,
        help="Maximum allowed regression mismatches before failing",
    )
    return parser


def run(argv: list[str] | None = None) -> int:
    """Run entity regression and return shell exit code."""
    args = _build_arg_parser().parse_args(argv)
    if args.max_regressions < 0:
        raise ValueError("--max-regressions must be >= 0")

    fixture = load_fixture(args.fixture)
    report = run_entity_regression(fixture)
    write_report(args.report_out, report)
    print(
        "Entity regression complete: "
        f"{report['regressions']} mismatches across {report['total_cases']} cases",
        file=sys.stdout,
    )
    return 1 if report["regressions"] > args.max_regressions else 0


def main() -> int:
    """Entry point."""
    try:
        return run()
    except (FileNotFoundError, ValueError) as exc:
        print(f"entity-regression error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
