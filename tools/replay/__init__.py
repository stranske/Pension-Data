"""Replay harness for golden-corpus regression checks."""

from tools.replay.diff_runner import run as run_diff
from tools.replay.harness import (
    REPLAY_BASELINE_ARTIFACT_TYPE,
    SUPPORTED_ARTIFACT_SCHEMA_VERSION,
    SUPPORTED_BASELINE_VERSION,
    CorpusDocument,
    DiffReport,
    EvaluationMetadata,
    ExtractionAccuracyReport,
    FieldDiff,
    FieldExtraction,
    ReplayResult,
    ReplaySnapshot,
    assert_accuracy_thresholds,
    build_snapshot,
    diff_snapshots,
    evaluate_extraction_accuracy,
    load_snapshot,
    run_replay,
    write_snapshot,
)
from tools.replay.promote_review_corrections import run as run_promote_review_corrections
from tools.replay.runner import load_corpus, load_parser, run

__all__ = [
    "REPLAY_BASELINE_ARTIFACT_TYPE",
    "SUPPORTED_ARTIFACT_SCHEMA_VERSION",
    "SUPPORTED_BASELINE_VERSION",
    "CorpusDocument",
    "DiffReport",
    "EvaluationMetadata",
    "ExtractionAccuracyReport",
    "FieldDiff",
    "FieldExtraction",
    "ReplayResult",
    "ReplaySnapshot",
    "build_snapshot",
    "assert_accuracy_thresholds",
    "diff_snapshots",
    "evaluate_extraction_accuracy",
    "load_snapshot",
    "load_corpus",
    "load_parser",
    "run_diff",
    "run_replay",
    "run",
    "run_promote_review_corrections",
    "write_snapshot",
]
