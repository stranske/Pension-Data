"""Tests for review-gated golden correction promotion."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.replay.harness import FieldExtraction, ReplayResult, build_snapshot, write_snapshot
from tools.replay.promote_review_corrections import run


def _baseline(path: Path) -> None:
    snapshot = build_snapshot(
        [ReplayResult(document_id="doc-a", fields={"funded_ratio": FieldExtraction(value=0.8)})]
    )
    snapshot["evaluation"] = {
        "corpus_id": "unit-corpus",
        "corpus_schema_version": 1,
        "thresholds": {
            "field_precision": 0.0,
            "field_recall": 0.0,
            "exact_match_accuracy": 0.0,
            "document_pass_rate": 0.0,
        },
    }
    write_snapshot(path, snapshot)


def test_promote_reviewed_correction_once_with_provenance(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.json"
    corrections = tmp_path / "corrections.json"
    output = tmp_path / "promoted.json"
    _baseline(baseline)
    corrections.write_text(
        json.dumps(
            [
                {
                    "correction_id": "review-1",
                    "document_id": "doc-a",
                    "field": "funded_ratio",
                    "value": 0.81,
                    "reviewer": "reviewer-a",
                    "state": "resolved",
                    "evidence_refs": ["p.44"],
                }
            ]
        ),
        encoding="utf-8",
    )

    assert (
        run(
            [
                "--baseline",
                str(baseline),
                "--corrections",
                str(corrections),
                "--output",
                str(output),
                "--baseline-update-ticket",
                "#836",
            ]
        )
        == 0
    )
    promoted = json.loads(output.read_text(encoding="utf-8"))
    assert promoted["documents"][0]["fields"]["funded_ratio"]["value"] == 0.81
    assert promoted["correction_provenance"][0]["correction_id"] == "review-1"
    assert promoted["correction_provenance"][0]["baseline_update_ticket"] == "#836"
    assert (
        run(
            [
                "--baseline",
                str(output),
                "--corrections",
                str(corrections),
                "--output",
                str(tmp_path / "again.json"),
                "--baseline-update-ticket",
                "#836",
            ]
        )
        == 1
    )


def test_promote_rejects_unreviewed_correction(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.json"
    corrections = tmp_path / "corrections.json"
    _baseline(baseline)
    corrections.write_text(
        json.dumps(
            [
                {
                    "correction_id": "review-2",
                    "document_id": "doc-a",
                    "field": "funded_ratio",
                    "value": 0.81,
                    "reviewer": "reviewer-a",
                    "state": "in_review",
                    "evidence_refs": ["p.44"],
                }
            ]
        ),
        encoding="utf-8",
    )
    assert (
        run(
            [
                "--baseline",
                str(baseline),
                "--corrections",
                str(corrections),
                "--output",
                str(tmp_path / "out.json"),
                "--baseline-update-ticket",
                "#836",
            ]
        )
        == 1
    )


@pytest.mark.parametrize(
    ("document_id", "field"),
    [("unknown-document", "funded_ratio"), ("doc-a", "unknown-field")],
)
def test_promote_rejects_unknown_golden_target(
    tmp_path: Path, document_id: str, field: str
) -> None:
    baseline = tmp_path / "baseline.json"
    corrections = tmp_path / "corrections.json"
    _baseline(baseline)
    corrections.write_text(
        json.dumps(
            [{
                "correction_id": "review-unknown-target",
                "document_id": document_id,
                "field": field,
                "value": 0.81,
                "reviewer": "reviewer-a",
                "state": "resolved",
                "evidence_refs": ["p.44"],
            }]
        ),
        encoding="utf-8",
    )

    assert run([
        "--baseline", str(baseline),
        "--corrections", str(corrections),
        "--output", str(tmp_path / "out.json"),
        "--baseline-update-ticket", "#836",
    ]) == 1
