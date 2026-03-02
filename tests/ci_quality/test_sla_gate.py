"""Tests for SLA threshold CI quality gate."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.ci_quality.sla_gate import (
    build_report,
    evaluate_sla,
    load_metrics,
    load_thresholds,
    run_gate,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_sla_gate_passes_when_all_critical_thresholds_hold(tmp_path: Path) -> None:
    metrics_path = tmp_path / "metrics.json"
    thresholds_path = tmp_path / "thresholds.json"
    report_path = tmp_path / "report.json"
    _write_json(
        metrics_path,
        {
            "completeness_rate": 0.98,
            "freshness_lag_hours": 8.0,
            "review_queue_latency_hours": 6.0,
        },
    )
    _write_json(
        thresholds_path,
        {
            "completeness_rate": {"op": ">=", "value": 0.95, "critical": True},
            "freshness_lag_hours": {"op": "<=", "value": 12.0, "critical": True},
            "review_queue_latency_hours": {"op": "<=", "value": 12.0, "critical": False},
        },
    )

    assert run_gate(
        metrics_path=metrics_path, thresholds_path=thresholds_path, report_path=report_path
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "pass"
    assert report["critical_breach_count"] == 0
    assert report["reason_counts"] == {}
    assert report["failed_critical_metrics"] == []
    assert report["failed_noncritical_metrics"] == []


def test_sla_gate_fails_on_missing_or_breached_critical_metrics(tmp_path: Path) -> None:
    metrics_path = tmp_path / "metrics.json"
    thresholds_path = tmp_path / "thresholds.json"
    _write_json(
        metrics_path,
        {
            "completeness_rate": 0.80,
            "freshness_lag_hours": 18.0,
        },
    )
    _write_json(
        thresholds_path,
        {
            "completeness_rate": {"op": ">=", "value": 0.95, "critical": True},
            "freshness_lag_hours": {"op": "<=", "value": 12.0, "critical": True},
            "review_queue_latency_hours": {"op": "<=", "value": 12.0, "critical": True},
        },
    )

    metrics = load_metrics(metrics_path)
    thresholds = load_thresholds(thresholds_path)
    breaches = evaluate_sla(metrics, thresholds)
    report = build_report(breaches)

    assert report["status"] == "fail"
    assert report["critical_breach_count"] == 3
    assert report["failed_critical_metrics"] == [
        "completeness_rate",
        "freshness_lag_hours",
        "review_queue_latency_hours",
    ]
    assert report["failed_noncritical_metrics"] == []
    assert report["reason_counts"] == {"missing_metric": 1, "threshold_breach": 2}
    assert {breach["reason"] for breach in report["breaches"]} == {
        "threshold_breach",
        "missing_metric",
    }


def test_sla_gate_tracks_noncritical_breach_summaries(tmp_path: Path) -> None:
    metrics_path = tmp_path / "metrics.json"
    thresholds_path = tmp_path / "thresholds.json"
    report_path = tmp_path / "report.json"
    _write_json(
        metrics_path,
        {
            "completeness_rate": 0.99,
            "review_queue_latency_hours": 24.0,
        },
    )
    _write_json(
        thresholds_path,
        {
            "completeness_rate": {"op": ">=", "value": 0.95, "critical": True},
            "review_queue_latency_hours": {"op": "<=", "value": 12.0, "critical": False},
        },
    )

    assert run_gate(
        metrics_path=metrics_path, thresholds_path=thresholds_path, report_path=report_path
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "pass"
    assert report["critical_breach_count"] == 0
    assert report["noncritical_breach_count"] == 1
    assert report["failed_critical_metrics"] == []
    assert report["failed_noncritical_metrics"] == ["review_queue_latency_hours"]
    assert report["reason_counts"] == {"threshold_breach": 1}


@pytest.mark.parametrize(
    "payload", [{"completeness_rate": True}, {"completeness_rate": float("nan")}]
)
def test_load_metrics_rejects_bool_and_nonfinite_values(
    tmp_path: Path, payload: dict[str, object]
) -> None:
    metrics_path = tmp_path / "metrics.json"
    _write_json(metrics_path, payload)

    with pytest.raises(ValueError, match="must be a finite numeric value"):
        load_metrics(metrics_path)


@pytest.mark.parametrize(
    "payload",
    [
        {"completeness_rate": {"op": ">=", "value": False, "critical": True}},
        {"completeness_rate": {"op": ">=", "value": float("inf"), "critical": True}},
    ],
)
def test_load_thresholds_rejects_bool_and_nonfinite_values(
    tmp_path: Path, payload: dict[str, object]
) -> None:
    thresholds_path = tmp_path / "thresholds.json"
    _write_json(thresholds_path, payload)

    with pytest.raises(ValueError, match="must be a finite numeric value"):
        load_thresholds(thresholds_path)
