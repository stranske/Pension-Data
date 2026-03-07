"""Gate one-PDF component coverage artifacts against core schema completeness."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pension_data.coverage.component_completeness import (
    CORE_SCHEMA_COMPONENTS,
    build_component_coverage_report_from_manifest,
)


def _write_report(report_path: Path, report: dict[str, object]) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_gate(
    *,
    component_manifest_path: Path,
    run_id: str | None = None,
    report_path: Path | None = None,
) -> bool:
    """Build and validate one-PDF component coverage report from a manifest artifact."""
    report = build_component_coverage_report_from_manifest(
        component_manifest_path=component_manifest_path,
        run_id=run_id,
    )
    if report_path is not None:
        _write_report(report_path, report)
    return bool(report.get("is_valid") is True)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        required=True,
        help="Path to extraction-persistence component datasets manifest JSON",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional run identifier to include in report output",
    )
    parser.add_argument(
        "--report-out",
        type=Path,
        default=None,
        help="Optional output path for machine-readable component coverage report",
    )
    return parser


def main() -> int:
    args = _build_arg_parser().parse_args()
    passed = run_gate(
        component_manifest_path=args.manifest,
        run_id=args.run_id,
        report_path=args.report_out,
    )
    if passed:
        return 0
    print(
        "Component coverage gate failed: at least one core schema component is missing or invalid "
        f"(expected={len(CORE_SCHEMA_COMPONENTS)})."
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
