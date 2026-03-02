"""Tests for fixture pipeline SLA threshold gate."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.ci_quality.fixture_pipeline_gate import run_fixture_pipeline_gate

FIXTURES_DIR = Path("tools/ci_quality/fixtures")


def test_fixture_pipeline_gate_passes_when_fixture_meets_critical_thresholds(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "fixture_pipeline_report.json"
    passed = run_fixture_pipeline_gate(
        thresholds_path=FIXTURES_DIR / "sla_thresholds.json",
        fixture_paths=[FIXTURES_DIR / "sla_metrics_ok.json"],
        report_path=report_path,
    )

    assert passed
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "pass"
    assert report["fixture_count"] == 1
    assert report["failing_fixture_count"] == 0
    assert report["failing_fixtures"] == []
    assert report["total_critical_breaches"] == 0


def test_fixture_pipeline_gate_fails_when_any_fixture_breaches_critical_thresholds(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "fixture_pipeline_report.json"
    passed = run_fixture_pipeline_gate(
        thresholds_path=FIXTURES_DIR / "sla_thresholds.json",
        fixture_paths=[
            FIXTURES_DIR / "sla_metrics_ok.json",
            FIXTURES_DIR / "sla_metrics_breach.json",
        ],
        report_path=report_path,
    )

    assert not passed
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "fail"
    assert report["fixture_count"] == 2
    assert report["failing_fixture_count"] == 1
    assert report["failing_fixtures"] == [str(FIXTURES_DIR / "sla_metrics_breach.json")]
    assert report["total_critical_breaches"] == 3
    failed_fixture_report = next(
        item for item in report["fixtures"] if item["fixture"].endswith("sla_metrics_breach.json")
    )
    assert failed_fixture_report["failed_critical_metrics"] == [
        "completeness_rate",
        "freshness_lag_hours",
        "review_queue_latency_hours",
    ]


def test_fixture_pipeline_gate_requires_at_least_one_fixture() -> None:
    with pytest.raises(ValueError, match="at least one fixture metrics path is required"):
        run_fixture_pipeline_gate(
            thresholds_path=FIXTURES_DIR / "sla_thresholds.json",
            fixture_paths=[],
        )
