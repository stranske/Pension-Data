"""Golden-corpus regression harness tests for extraction fallback parser."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from tools.replay.harness import (
    assert_accuracy_thresholds,
    build_snapshot,
    diff_snapshots,
    evaluate_extraction_accuracy,
    load_snapshot,
    run_replay,
)
from tools.replay.runner import load_corpus, load_parser


def test_golden_corpus_snapshot_matches_baseline_without_unexpected_drift() -> None:
    root = Path(__file__).resolve().parents[2]
    corpus_path = root / "tests" / "golden" / "extraction_fallback_corpus.json"
    baseline_path = root / "tests" / "golden" / "extraction_fallback_baseline.json"

    corpus = load_corpus(corpus_path)
    parser = load_parser("tools.golden_extract.fallback_extract_parser:parse")
    replay_results = run_replay(corpus, parser)
    snapshot = build_snapshot(
        replay_results,
        parser_id="tools.golden_extract.fallback_extract_parser:parse",
        generated_at=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
    )
    snapshot["evaluation"] = {
        "corpus_id": "extraction_fallback_corpus",
        "corpus_schema_version": 1,
        "thresholds": {
            "field_precision": 1.0,
            "field_recall": 1.0,
            "exact_match_accuracy": 1.0,
            "document_pass_rate": 1.0,
        },
    }
    baseline = load_snapshot(baseline_path)
    report = diff_snapshots(baseline=baseline, current=snapshot)

    assert report["unexpected_changes"] == 0
    accuracy = evaluate_extraction_accuracy(baseline=baseline, current=snapshot)
    assert_accuracy_thresholds(baseline=baseline, report=accuracy)
    assert accuracy["corpus_size"] == 5
    assert accuracy["field_precision"] == 1.0
