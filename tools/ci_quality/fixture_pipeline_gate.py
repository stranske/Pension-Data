"""Validate core SLA thresholds across fixture pipeline metric artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import TypedDict

from tools.ci_quality.sla_gate import build_report, evaluate_sla, load_metrics, load_thresholds


class FixtureResult(TypedDict):
    """Per-fixture SLA gate result."""

    fixture: str
    status: str
    critical_breach_count: int
    noncritical_breach_count: int
    reason_counts: dict[str, int]
    failed_critical_metrics: list[str]
    failed_noncritical_metrics: list[str]


class FixturePipelineGateReport(TypedDict):
    """Aggregated fixture-pipeline SLA gate output."""

    status: str
    fixture_count: int
    failing_fixture_count: int
    total_critical_breaches: int
    failing_fixtures: list[str]
    fixtures: list[FixtureResult]


def run_fixture_pipeline_gate(
    *,
    thresholds_path: Path,
    fixture_paths: list[Path],
    report_path: Path | None = None,
) -> bool:
    """Run SLA threshold validation across fixture metric artifacts."""
    if not fixture_paths:
        raise ValueError("at least one fixture metrics path is required")

    thresholds = load_thresholds(thresholds_path)
    fixture_reports: list[FixtureResult] = []
    for fixture_path in sorted(fixture_paths):
        metrics = load_metrics(fixture_path)
        report = build_report(evaluate_sla(metrics, thresholds))
        fixture_reports.append(
            {
                "fixture": str(fixture_path),
                "status": report["status"],
                "critical_breach_count": report["critical_breach_count"],
                "noncritical_breach_count": report["noncritical_breach_count"],
                "reason_counts": report["reason_counts"],
                "failed_critical_metrics": report["failed_critical_metrics"],
                "failed_noncritical_metrics": report["failed_noncritical_metrics"],
            }
        )

    failing_fixtures = [
        item["fixture"] for item in fixture_reports if item["critical_breach_count"] > 0
    ]
    full_report: FixturePipelineGateReport = {
        "status": "fail" if failing_fixtures else "pass",
        "fixture_count": len(fixture_reports),
        "failing_fixture_count": len(failing_fixtures),
        "total_critical_breaches": sum(item["critical_breach_count"] for item in fixture_reports),
        "failing_fixtures": failing_fixtures,
        "fixtures": fixture_reports,
    }

    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(full_report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    return not failing_fixtures


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--thresholds", type=Path, required=True, help="Path to thresholds JSON")
    parser.add_argument(
        "--fixtures",
        type=Path,
        nargs="+",
        required=True,
        help="One or more fixture metrics JSON files",
    )
    parser.add_argument(
        "--report-out",
        type=Path,
        default=None,
        help="Optional output path for fixture pipeline gate report JSON",
    )
    return parser


def main() -> int:
    args = _build_arg_parser().parse_args()
    passed = run_fixture_pipeline_gate(
        thresholds_path=args.thresholds,
        fixture_paths=args.fixtures,
        report_path=args.report_out,
    )
    if passed:
        return 0
    print("Fixture pipeline SLA gate failed: one or more fixtures breached critical thresholds.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
