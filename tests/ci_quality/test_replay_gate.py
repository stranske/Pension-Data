"""Tests for replay regression quality gate."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.ci_quality.replay_gate import (
    build_report,
    evaluate_replay_diff,
    load_replay_diff,
    run_gate,
    validate_summary_consistency,
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
            "changes": [{"classification": "expected_change"}, {"classification": "format_only"}],
        },
    )

    assert run_gate(diff_path=diff_path, max_unexpected=0, report_path=report_path)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "pass"
    assert report["unexpected_changes"] == 0
    assert report["classification_counts"] == {"expected_change": 1, "format_only": 1}
    assert report["unexpected_examples"] == []


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


def test_replay_gate_report_includes_unexpected_change_examples(tmp_path: Path) -> None:
    diff_path = tmp_path / "diff.json"
    report_path = tmp_path / "report.json"
    _write_json(
        diff_path,
        {
            "changes": [
                {"classification": "unexpected_drift", "path": "a.json", "field": "name"},
                {"classification": "unexpected_drift", "path": "b.json", "field": "status"},
                {"classification": "expected_change", "path": "c.json", "field": "id"},
            ]
        },
    )

    assert not run_gate(diff_path=diff_path, max_unexpected=0, report_path=report_path)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["classification_counts"] == {"expected_change": 1, "unexpected_drift": 2}
    assert report["unexpected_examples"] == [
        {"classification": "unexpected_drift", "field": "name", "path": "a.json"},
        {"classification": "unexpected_drift", "field": "status", "path": "b.json"},
    ]


def test_replay_gate_fails_when_summary_disagrees_with_change_details(tmp_path: Path) -> None:
    diff_path = tmp_path / "diff.json"
    report_path = tmp_path / "report.json"
    _write_json(
        diff_path,
        {
            "total_changes": 0,
            "unexpected_changes": 0,
            "changes": [
                {"classification": "unexpected_drift", "path": "a.json"},
                {"classification": "expected_change", "path": "b.json"},
            ],
        },
    )

    assert not run_gate(diff_path=diff_path, max_unexpected=5, report_path=report_path)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "fail"
    assert any("unexpected_changes does not match" in message for message in report["violations"])
    assert any("total_changes does not match" in message for message in report["violations"])


def test_validate_summary_consistency_allows_matching_summary() -> None:
    violations = validate_summary_consistency(
        {
            "total_changes": 2,
            "unexpected_changes": 1,
            "changes": [
                {"classification": "unexpected_drift"},
                {"classification": "expected_change"},
            ],
        }
    )
    assert violations == []


def test_replay_gate_rejects_negative_tolerance(tmp_path: Path) -> None:
    diff_path = tmp_path / "diff.json"
    _write_json(diff_path, {"changes": []})
    with pytest.raises(ValueError, match="max_unexpected must be >= 0"):
        run_gate(diff_path=diff_path, max_unexpected=-1)
