"""Tests for replay diff CLI classification workflow."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from tools.replay.diff_runner import run
from tools.replay.harness import (
    CorpusDocument,
    FieldExtraction,
    ReplayResult,
    build_snapshot,
    run_replay,
    write_snapshot,
)


def _parser(document: CorpusDocument) -> dict[str, FieldExtraction]:
    if document.document_id == "doc-a":
        return {"funded_ratio": FieldExtraction(value=0.80, confidence=0.95, evidence="p1")}
    return {"funded_ratio": FieldExtraction(value=0.72, confidence=0.91, evidence="p4")}


def _write_snapshot(path: Path, *, funded_ratio: float) -> None:
    snapshot = build_snapshot(
        [
            ReplayResult(
                document_id="doc-a",
                fields={
                    "funded_ratio": FieldExtraction(
                        value=funded_ratio, confidence=0.95, evidence="p1"
                    )
                },
            )
        ],
        generated_at=datetime(2026, 3, 2, 0, 0, tzinfo=UTC),
    )
    write_snapshot(path, snapshot)


def test_diff_runner_succeeds_when_all_changes_are_expected(tmp_path: Path) -> None:
    baseline_path = tmp_path / "baseline.json"
    current_path = tmp_path / "current.json"
    expected_path = tmp_path / "expected.json"
    report_path = tmp_path / "report.json"

    _write_snapshot(baseline_path, funded_ratio=0.80)
    _write_snapshot(current_path, funded_ratio=0.81)
    expected_path.write_text(
        json.dumps({"expected_changes": [{"document_id": "doc-a", "field": "funded_ratio"}]}),
        encoding="utf-8",
    )

    exit_code = run(
        [
            "--baseline",
            str(baseline_path),
            "--current",
            str(current_path),
            "--expected-changes",
            str(expected_path),
            "--report-out",
            str(report_path),
        ]
    )

    assert exit_code == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["total_changes"] == 1
    assert report["expected_changes"] == 1
    assert report["unexpected_changes"] == 0
    assert report["changes"][0]["classification"] == "expected_change"


def test_diff_runner_fails_when_unexpected_drift_exists(tmp_path: Path) -> None:
    baseline_path = tmp_path / "baseline.json"
    current_path = tmp_path / "current.json"
    _write_snapshot(baseline_path, funded_ratio=0.80)
    _write_snapshot(current_path, funded_ratio=0.81)

    exit_code = run(["--baseline", str(baseline_path), "--current", str(current_path)])
    assert exit_code == 2


def test_diff_runner_rejects_invalid_expected_changes_file(tmp_path: Path) -> None:
    corpus = [CorpusDocument(document_id="doc-a", content="alpha")]
    replay_results = run_replay(corpus, _parser)
    baseline = build_snapshot(replay_results, generated_at=datetime(2026, 3, 2, 0, 0, tzinfo=UTC))
    baseline_path = tmp_path / "baseline.json"
    current_path = tmp_path / "current.json"
    write_snapshot(baseline_path, baseline)
    write_snapshot(current_path, baseline)

    expected_path = tmp_path / "expected.json"
    expected_path.write_text(json.dumps({"expected_changes": [{"document_id": "doc-a"}]}), "utf-8")

    exit_code = run(
        [
            "--baseline",
            str(baseline_path),
            "--current",
            str(current_path),
            "--expected-changes",
            str(expected_path),
        ]
    )
    assert exit_code == 1
