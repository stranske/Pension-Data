"""Tests for replay regression quality gate."""

from __future__ import annotations

import json
from pathlib import Path

from tools.ci_quality.replay_gate import (
    build_report,
    evaluate_replay_diff,
    load_replay_diff,
    run_gate,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_replay_gate_passes_when_unexpected_within_tolerance(tmp_path: Path) -> None:
    diff_path = tmp_path / "diff.json"
    report_path = tmp_path / "report.json"
    _write_json(
        diff_path,
        {
            "total_changes": 2,
            "unexpected_changes": 0,
            "changes": [],
        },
    )

    assert run_gate(diff_path=diff_path, max_unexpected=0, report_path=report_path)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "pass"
    assert report["unexpected_changes"] == 0


def test_replay_gate_fails_when_unexpected_exceeds_tolerance(tmp_path: Path) -> None:
    diff_path = tmp_path / "diff.json"
    _write_json(
        diff_path,
        {
            "changes": [
                {"classification": "unexpected_drift"},
                {"classification": "unexpected_drift"},
                {"classification": "expected_change"},
            ]
        },
    )

    total, unexpected = load_replay_diff(diff_path)
    violations = evaluate_replay_diff(unexpected_changes=unexpected, max_unexpected=1)
    report = build_report(
        total_changes=total,
        unexpected_changes=unexpected,
        max_unexpected=1,
        violations=violations,
    )

    assert unexpected == 2
    assert report["status"] == "fail"
    assert "exceeds tolerance" in report["violations"][0]
