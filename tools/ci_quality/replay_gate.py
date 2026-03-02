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
    classification_counts: dict[str, int]
    unexpected_examples: list[dict[str, object]]


def load_replay_diff(diff_path: Path) -> tuple[int, int]:
    """Load replay diff and return total and unexpected change counts."""
    payload = load_replay_payload(diff_path)

    declared_unexpected = payload.get("unexpected_changes")
    if declared_unexpected is not None and not isinstance(declared_unexpected, int):
        raise ValueError("replay diff unexpected_changes must be an integer")

    computed_total, computed_unexpected = _compute_counts_from_changes(payload)

    unexpected = (
        declared_unexpected if isinstance(declared_unexpected, int) else computed_unexpected
    )

    total = payload.get("total_changes")
    if total is not None and not isinstance(total, int):
        raise ValueError("replay diff total_changes must be an integer")
    total_changes = total if isinstance(total, int) else computed_total
    return total_changes, unexpected


def load_replay_payload(diff_path: Path) -> dict[str, object]:
    """Load and validate replay diff JSON payload."""
    payload = json.loads(diff_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("replay diff payload must be a JSON object")
    return payload


def _extract_changes(payload: dict[str, object]) -> list[dict[str, object]]:
    raw_changes = payload.get("changes", [])
    if raw_changes is None:
        return []
    if not isinstance(raw_changes, list):
        raise ValueError("replay diff changes must be a list")
    return [item for item in raw_changes if isinstance(item, dict)]


def _compute_counts_from_changes(payload: dict[str, object]) -> tuple[int, int]:
    changes = _extract_changes(payload)
    unexpected = sum(1 for item in changes if item.get("classification") == "unexpected_drift")
    return len(changes), unexpected


def validate_summary_consistency(payload: dict[str, object]) -> list[str]:
    """Validate summary fields against detailed replay changes."""
    violations: list[str] = []
    computed_total, computed_unexpected = _compute_counts_from_changes(payload)

    declared_total = payload.get("total_changes")
    if isinstance(declared_total, int) and declared_total != computed_total:
        violations.append(
            "replay diff total_changes does not match number of detailed changes: "
            f"{declared_total} != {computed_total}"
        )

    declared_unexpected = payload.get("unexpected_changes")
    if isinstance(declared_unexpected, int) and declared_unexpected != computed_unexpected:
        violations.append(
            "replay diff unexpected_changes does not match unexpected_drift entries: "
            f"{declared_unexpected} != {computed_unexpected}"
        )

    return violations


def summarize_replay_changes(payload: dict[str, object]) -> dict[str, int]:
    """Build classification counts for replay change details."""
    changes = _extract_changes(payload)
    counts: dict[str, int] = {}
    for item in changes:
        classification = item.get("classification")
        key = classification if isinstance(classification, str) else "unclassified"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def sample_unexpected_changes(
    payload: dict[str, object], *, limit: int = 5
) -> list[dict[str, object]]:
    """Collect a bounded sample of unexpected drift entries for quick triage."""
    changes = _extract_changes(payload)
    examples = [item for item in changes if item.get("classification") == "unexpected_drift"]
    return examples[:limit]


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
    classification_counts: dict[str, int] | None = None,
    unexpected_examples: list[dict[str, object]] | None = None,
) -> ReplayGateReport:
    """Build CI artifact payload for replay quality gate."""
    return {
        "status": "fail" if violations else "pass",
        "total_changes": total_changes,
        "unexpected_changes": unexpected_changes,
        "max_unexpected": max_unexpected,
        "violations": violations,
        "classification_counts": classification_counts or {},
        "unexpected_examples": unexpected_examples or [],
    }


def write_report(report_path: Path, report: ReplayGateReport) -> None:
    """Write replay gate report to artifact path."""
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_gate(*, diff_path: Path, max_unexpected: int, report_path: Path | None = None) -> bool:
    """Run replay quality gate and optionally emit artifact report."""
    if max_unexpected < 0:
        raise ValueError("max_unexpected must be >= 0")

    payload = load_replay_payload(diff_path)
    total_changes, unexpected_changes = load_replay_diff(diff_path)
    violations = evaluate_replay_diff(
        unexpected_changes=unexpected_changes,
        max_unexpected=max_unexpected,
    )
    violations.extend(validate_summary_consistency(payload))
    report = build_report(
        total_changes=total_changes,
        unexpected_changes=unexpected_changes,
        max_unexpected=max_unexpected,
        violations=violations,
        classification_counts=summarize_replay_changes(payload),
        unexpected_examples=sample_unexpected_changes(payload),
    )
    if report_path is not None:
        write_report(report_path, report)
    return not violations


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--diff", type=Path, required=True, help="Path to replay diff JSON artifact"
    )
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
