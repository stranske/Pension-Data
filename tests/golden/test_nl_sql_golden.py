"""Golden gate test for the NL->SQL reference run (provenance + UNSAFE_SQL + replay)."""

from __future__ import annotations

import json
from pathlib import Path

from tools.golden_nl.nl_sql_golden import (
    _seed_replay_fixture,
    diff_snapshot,
    run,
    run_corpus,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CORPUS = _REPO_ROOT / "tests" / "golden" / "nl_sql" / "corpus.json"
_BASELINE = _REPO_ROOT / "tests" / "golden" / "nl_sql" / "baseline.json"


def _load_corpus() -> dict[str, object]:
    return json.loads(_CORPUS.read_text(encoding="utf-8"))


def test_corpus_and_baseline_are_committed() -> None:
    assert _CORPUS.exists()
    assert _BASELINE.exists()


def test_corpus_run_has_no_violations_and_matches_baseline() -> None:
    snapshot, violations = run_corpus(_load_corpus())
    assert violations == []
    baseline = json.loads(_BASELINE.read_text(encoding="utf-8"))
    report = diff_snapshot(baseline=baseline, snapshot=snapshot)
    assert report["unexpected_changes"] == 0, report


def test_unsafe_sql_and_provenance_invariants() -> None:
    snapshot, violations = run_corpus(_load_corpus())
    assert violations == []
    by_id = {row["id"]: row for row in snapshot}

    unsafe = by_id["omits_source_document_id"]
    assert unsafe["status"] == "error"
    assert unsafe["error_code"] == "UNSAFE_SQL"

    for safe_id in ("funded_ratio_with_provenance", "all_curated_metric_facts"):
        row = by_id[safe_id]
        assert row["status"] == "ok"
        assert row["error_code"] is None
        assert row["returned_rows"] > 0
        assert row["provenance_complete"] is True


def test_baseline_drift_is_detected() -> None:
    snapshot, _ = run_corpus(_load_corpus())
    baseline = json.loads(_BASELINE.read_text(encoding="utf-8"))
    for row in baseline["queries"]:
        if row["id"] == "funded_ratio_with_provenance":
            row["returned_rows"] = row["returned_rows"] + 100
    report = diff_snapshot(baseline=baseline, snapshot=snapshot)
    assert report["unexpected_changes"] > 0


def test_recorded_request_replays_deterministically(tmp_path: Path) -> None:
    # _seed_replay_fixture raises if the recorded request does not replay to the
    # same status / returned_rows against the seeded SQLite fixture.
    expected = _seed_replay_fixture(_load_corpus(), tmp_path)
    assert expected["status"] == "ok"
    assert expected["returned_rows"] == 2
    assert (tmp_path / "seed.db").exists()
    assert (tmp_path / "nl_operations.jsonl").exists()


def test_run_cli_produces_snapshot_diff_and_replay_artifacts(tmp_path: Path) -> None:
    """run() is the entry point the CI workflow invokes; verify it emits required artifacts."""
    rc = run(
        [
            "--corpus",
            str(_CORPUS),
            "--baseline",
            str(_BASELINE),
            "--emit-root",
            str(tmp_path),
        ]
    )

    assert rc == 0, "clean corpus against matching baseline must return 0"
    assert (tmp_path / "snapshot.json").exists()
    assert (tmp_path / "diff_report.json").exists()
    assert (tmp_path / "replay_expected.json").exists()
    assert (tmp_path / "seed.db").exists()
    assert (tmp_path / "nl_operations.jsonl").exists()

    diff = json.loads((tmp_path / "diff_report.json").read_text())
    assert diff["unexpected_changes"] == 0


def test_run_cli_returns_two_when_baseline_drifts(tmp_path: Path) -> None:
    """run() returns exit code 2 (drift) when the baseline does not match current output."""
    drifted = tmp_path / "drifted_baseline.json"
    baseline = json.loads(_BASELINE.read_text())
    for row in baseline["queries"]:
        if row["id"] == "funded_ratio_with_provenance":
            row["returned_rows"] = row["returned_rows"] + 999
    drifted.write_text(json.dumps(baseline) + "\n", encoding="utf-8")

    rc = run(
        [
            "--corpus",
            str(_CORPUS),
            "--baseline",
            str(drifted),
            "--emit-root",
            str(tmp_path),
        ]
    )

    assert rc == 2
    diff = json.loads((tmp_path / "diff_report.json").read_text())
    assert diff["unexpected_changes"] > 0
