"""CLI for replay snapshot diffs with expected vs unexpected drift classification."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from tools.replay.harness import diff_snapshots, load_snapshot


def _parse_expected_changes(path: Path) -> set[tuple[str, str]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("expected_changes") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        raise ValueError(
            "expected changes file must be a list or object with 'expected_changes' list"
        )

    expected: set[tuple[str, str]] = set()
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise ValueError(f"expected changes row {index} must be an object")
        document_id = row.get("document_id")
        field = row.get("field")
        if not isinstance(document_id, str) or not document_id.strip():
            raise ValueError(f"expected changes row {index} missing non-empty string 'document_id'")
        if not isinstance(field, str) or not field.strip():
            raise ValueError(f"expected changes row {index} missing non-empty string 'field'")
        expected.add((document_id, field))
    return expected


def run(argv: list[str] | None = None) -> int:
    """Execute snapshot diff and classify drift."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=Path, help="Baseline snapshot JSON path")
    parser.add_argument("--current", required=True, type=Path, help="Current snapshot JSON path")
    parser.add_argument(
        "--expected-changes",
        type=Path,
        help="Optional JSON file listing expected changed fields",
    )
    parser.add_argument(
        "--report-out",
        type=Path,
        help="Optional path for machine-readable diff report JSON",
    )
    args = parser.parse_args(argv)

    try:
        baseline = load_snapshot(args.baseline)
        current = load_snapshot(args.current)
        expected = (
            _parse_expected_changes(args.expected_changes)
            if args.expected_changes is not None
            else set()
        )
        report = diff_snapshots(baseline=baseline, current=current, expected_change_fields=expected)
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        print(f"replay-diff error: {exc}", file=sys.stderr)
        return 1

    if args.report_out is not None:
        args.report_out.parent.mkdir(parents=True, exist_ok=True)
        args.report_out.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

    print(
        "Replay diff complete: "
        f"{report['total_changes']} changes "
        f"({report['expected_changes']} expected, {report['unexpected_changes']} unexpected)",
        file=sys.stdout,
    )
    return 2 if report["unexpected_changes"] > 0 else 0


def main() -> int:
    """Entry point."""
    return run()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
