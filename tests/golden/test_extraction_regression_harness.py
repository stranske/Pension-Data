"""Golden-corpus regression harness tests for extraction fallback parser."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from tools.replay.harness import build_snapshot, diff_snapshots, load_snapshot, run_replay
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
    baseline = load_snapshot(baseline_path)
    report = diff_snapshots(baseline=baseline, current=snapshot)

    assert report["unexpected_changes"] == 0
