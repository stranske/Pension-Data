"""Gate fixture SLA metrics against configured thresholds."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Literal, TypedDict

ComparisonOperator = Literal[">=", "<=", ">", "<", "=="]


class ThresholdRule(TypedDict):
    """Threshold rule for a single SLA metric."""

    op: ComparisonOperator
    value: float
    critical: bool


class Breach(TypedDict):
    """Single threshold breach output."""

    metric: str
    reason: str
    critical: bool
    observed: float | None
    threshold: float
    op: ComparisonOperator


class SLAGateReport(TypedDict):
    """Structured SLA gate output for CI artifact upload."""

    status: str
    critical_breach_count: int
    noncritical_breach_count: int
    breaches: list[Breach]


def load_metrics(metrics_path: Path) -> dict[str, float]:
    """Load numeric SLA metrics map."""
    payload = json.loads(metrics_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("metrics payload must be a JSON object")
    metrics: dict[str, float] = {}
    for key, value in payload.items():
        if not isinstance(key, str):
            raise ValueError("metric name must be a string")
        if not isinstance(value, (int, float)):
            raise ValueError(f"metric '{key}' must be numeric")
        metrics[key] = float(value)
    return metrics


def load_thresholds(thresholds_path: Path) -> dict[str, ThresholdRule]:
    """Load threshold-rule map from JSON."""
    payload = json.loads(thresholds_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("thresholds payload must be a JSON object")
    rules: dict[str, ThresholdRule] = {}
    for metric, raw_rule in payload.items():
        if not isinstance(metric, str):
            raise ValueError("threshold metric name must be a string")
        if not isinstance(raw_rule, dict):
            raise ValueError(f"threshold for '{metric}' must be an object")
        op = raw_rule.get("op")
        value = raw_rule.get("value")
        critical = raw_rule.get("critical", True)
        if op not in {">=", "<=", ">", "<", "=="}:
            raise ValueError(f"threshold op for '{metric}' must be one of >= <= > < ==")
        if not isinstance(value, (int, float)):
            raise ValueError(f"threshold value for '{metric}' must be numeric")
        if not isinstance(critical, bool):
            raise ValueError(f"threshold critical for '{metric}' must be boolean")
        rules[metric] = {
            "op": op,
            "value": float(value),
            "critical": critical,
        }
    return rules


def _passes_threshold(*, observed: float, op: ComparisonOperator, threshold: float) -> bool:
    if op == ">=":
        return observed >= threshold
    if op == "<=":
        return observed <= threshold
    if op == ">":
        return observed > threshold
    if op == "<":
        return observed < threshold
    return observed == threshold


def evaluate_sla(metrics: dict[str, float], thresholds: dict[str, ThresholdRule]) -> list[Breach]:
    """Return list of SLA breaches (critical and non-critical)."""
    breaches: list[Breach] = []
    for metric_name in sorted(thresholds.keys()):
        rule = thresholds[metric_name]
        observed = metrics.get(metric_name)
        if observed is None:
            breaches.append(
                {
                    "metric": metric_name,
                    "reason": "missing_metric",
                    "critical": rule["critical"],
                    "observed": None,
                    "threshold": rule["value"],
                    "op": rule["op"],
                }
            )
            continue
        if not _passes_threshold(observed=observed, op=rule["op"], threshold=rule["value"]):
            breaches.append(
                {
                    "metric": metric_name,
                    "reason": "threshold_breach",
                    "critical": rule["critical"],
                    "observed": observed,
                    "threshold": rule["value"],
                    "op": rule["op"],
                }
            )
    return breaches


def build_report(breaches: list[Breach]) -> SLAGateReport:
    """Build machine-readable SLA gate report."""
    critical_count = sum(1 for item in breaches if item["critical"])
    noncritical_count = len(breaches) - critical_count
    return {
        "status": "fail" if critical_count else "pass",
        "critical_breach_count": critical_count,
        "noncritical_breach_count": noncritical_count,
        "breaches": breaches,
    }


def write_report(report_path: Path, report: SLAGateReport) -> None:
    """Write SLA gate report artifact."""
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_gate(
    *,
    metrics_path: Path,
    thresholds_path: Path,
    report_path: Path | None = None,
) -> bool:
    """Run SLA gate against metrics and thresholds."""
    metrics = load_metrics(metrics_path)
    thresholds = load_thresholds(thresholds_path)
    breaches = evaluate_sla(metrics, thresholds)
    report = build_report(breaches)
    if report_path is not None:
        write_report(report_path, report)
    return report["critical_breach_count"] == 0


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metrics", type=Path, required=True, help="Path to metrics JSON")
    parser.add_argument("--thresholds", type=Path, required=True, help="Path to thresholds JSON")
    parser.add_argument(
        "--report-out",
        type=Path,
        default=None,
        help="Optional output path for SLA gate report JSON",
    )
    return parser


def main() -> int:
    args = _build_arg_parser().parse_args()
    passed = run_gate(
        metrics_path=args.metrics,
        thresholds_path=args.thresholds,
        report_path=args.report_out,
    )
    if passed:
        return 0
    print("SLA quality gate failed: one or more critical thresholds were breached.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
