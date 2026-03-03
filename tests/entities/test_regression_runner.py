"""Tests for entity regression CLI runner behavior."""

from __future__ import annotations

import json
from pathlib import Path

from tools.entity_regression.runner import run

FIXTURE_PATH = Path(__file__).parent / "golden" / "entity_regression_cases.json"


def test_runner_succeeds_for_baseline_fixture(tmp_path: Path) -> None:
    report_path = tmp_path / "entity_regression_report.json"
    exit_code = run(
        [
            "--fixture",
            str(FIXTURE_PATH),
            "--report-out",
            str(report_path),
        ]
    )
    assert exit_code == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["regressions"] == 0
    assert report["total_cases"] == 7


def test_runner_fails_when_regressions_exceed_tolerance(tmp_path: Path) -> None:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    payload["lineage_cases"][0]["expected"]["terminal_entities"] = ["mgr:legacy_asset_rebrand"]

    modified_fixture = tmp_path / "entity_regression_cases_modified.json"
    modified_fixture.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    report_path = tmp_path / "entity_regression_report.json"
    exit_code = run(
        [
            "--fixture",
            str(modified_fixture),
            "--report-out",
            str(report_path),
            "--max-regressions",
            "0",
        ]
    )

    assert exit_code == 1
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["regressions"] == 1
    assert report["mismatches"][0]["suite"] == "lineage"
