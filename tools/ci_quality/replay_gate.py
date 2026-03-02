"""Gate replay diff artifacts against an unexpected-drift tolerance."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import TypedDict


class ReplayGateReport(TypedDict):
    """Structured replay gate output saved as CI artifact."""

    status: str
    total_changes: int
    unexpected_changes: int
    max_unexpected: int
    violations: list[str]


def load_replay_diff(diff_path: Path) -> tuple[int, int]:
    """Load replay diff and return total and unexpected change counts."""
    payload = json.loads(diff_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("replay diff payload must be a JSON object")

    if "unexpected_changes" in payload:
        unexpected = payload["unexpected_changes"]
        if not isinstance(unexpected, int):
            raise ValueError("replay diff unexpected_changes must be an integer")
    else:
        changes = payload.get("changes", [])
        if not isinstance(changes, list):
            raise ValueError("replay diff changes must be a list")
        unexpected = 0
        for item in changes:
            if not isinstance(item, dict):
                continue
            if item.get("classification") == "unexpected_drift":
                unexpected += 1

    total = payload.get("total_changes")
    if isinstance(total, int):
        total_changes = total
    else:
        changes = payload.get("changes", [])
        total_changes = len(changes) if isinstance(changes, list) else unexpected
    return total_changes, unexpected


def evaluate_replay_diff(*, unexpected_changes: int, max_unexpected: int) -> list[str]:
    """Return replay-gate violations for current tolerance settings."""
    violations: list[str] = []
    if unexpected_changes > max_unexpected:
        violations.append(
            f"unexpected replay drift {unexpected_changes} exceeds tolerance {max_unexpected}"
        )
    return violations


def build_report(
    *,
    total_changes: int,
    unexpected_changes: int,
    max_unexpected: int,
    violations: list[str],
) -> ReplayGateReport:
    """Build CI artifact payload for replay quality gate."""
    return {
        "status": "fail" if violations else "pass",
        "total_changes": total_changes,
        "unexpected_changes": unexpected_changes,
        "max_unexpected": max_unexpected,
        "violations": violations,
    }


def write_report(report_path: Path, report: ReplayGateReport) -> None:
    """Write replay gate report to artifact path."""
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_gate(*, diff_path: Path, max_unexpected: int, report_path: Path | None = None) -> bool:
    """Run replay quality gate and optionally emit artifact report."""
    total_changes, unexpected_changes = load_replay_diff(diff_path)
    violations = evaluate_replay_diff(
        unexpected_changes=unexpected_changes,
        max_unexpected=max_unexpected,
    )
    report = build_report(
        total_changes=total_changes,
        unexpected_changes=unexpected_changes,
        max_unexpected=max_unexpected,
        violations=violations,
    )
    if report_path is not None:
        write_report(report_path, report)
    return not violations


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--diff", type=Path, required=True, help="Path to replay diff JSON artifact")
    parser.add_argument(
        "--max-unexpected",
        type=int,
        default=0,
        help="Maximum allowed unexpected drift count before failing CI",
    )
    parser.add_argument(
        "--report-out",
        type=Path,
        default=None,
        help="Optional path for machine-readable replay gate report JSON",
    )
    return parser


def main() -> int:
    args = _build_arg_parser().parse_args()
    passed = run_gate(
        diff_path=args.diff,
        max_unexpected=args.max_unexpected,
        report_path=args.report_out,
    )
    if passed:
        return 0
    print("Replay quality gate failed: unexpected drift exceeds configured tolerance.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
