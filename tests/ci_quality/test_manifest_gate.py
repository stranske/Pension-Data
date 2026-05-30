"""Unit tests for the one-PDF pilot manifest golden gate."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.ci_quality.manifest_gate import diff_manifest, extract_snapshot, run

_BASELINE = {
    "artifact_files_keys": ["alpha_json", "beta_json", "gamma_json"],
    "ledger_status": "success",
}


def _manifest(*, keys: list[str], ledger_status: str) -> dict[str, object]:
    return {
        "ledger_status": ledger_status,
        "artifact_files": {key: f"/run/{key}.json" for key in keys},
    }


def test_extract_snapshot_returns_sorted_keys_and_status() -> None:
    snapshot = extract_snapshot(_manifest(keys=["gamma_json", "alpha_json"], ledger_status="ok"))
    assert snapshot == {
        "artifact_files_keys": ["alpha_json", "gamma_json"],
        "ledger_status": "ok",
    }


def test_extract_snapshot_rejects_malformed_manifest() -> None:
    with pytest.raises(ValueError, match="artifact_files"):
        extract_snapshot({"ledger_status": "success", "artifact_files": []})
    with pytest.raises(ValueError, match="ledger_status"):
        extract_snapshot({"artifact_files": {}})


def test_clean_manifest_has_no_drift() -> None:
    manifest = _manifest(keys=["alpha_json", "beta_json", "gamma_json"], ledger_status="success")
    report = diff_manifest(baseline=_BASELINE, current_manifest=manifest)
    assert report["unexpected_changes"] == 0
    assert report["total_changes"] == 0
    assert report["changes"] == []


def test_added_and_removed_artifact_keys_are_unexpected_drift() -> None:
    manifest = _manifest(keys=["alpha_json", "beta_json", "delta_json"], ledger_status="success")
    report = diff_manifest(baseline=_BASELINE, current_manifest=manifest)
    assert report["unexpected_changes"] == 2
    fields = {change["field"] for change in report["changes"]}
    assert fields == {"gamma_json", "delta_json"}
    assert all(change["classification"] == "unexpected_drift" for change in report["changes"])


def test_ledger_status_change_is_unexpected_drift() -> None:
    manifest = _manifest(keys=["alpha_json", "beta_json", "gamma_json"], ledger_status="partial")
    report = diff_manifest(baseline=_BASELINE, current_manifest=manifest)
    assert report["unexpected_changes"] == 1
    change = report["changes"][0]
    assert change["field"] == "ledger_status"
    assert change["baseline"] == "success"
    assert change["current"] == "partial"


def test_report_summary_counts_are_consistent() -> None:
    manifest = _manifest(keys=["alpha_json"], ledger_status="partial")
    report = diff_manifest(baseline=_BASELINE, current_manifest=manifest)
    # 2 removed keys + 1 ledger status change
    assert report["total_changes"] == 3
    assert report["unexpected_changes"] == len(report["changes"])


def _write(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")


def test_run_returns_zero_when_manifest_matches_baseline(tmp_path: Path) -> None:
    manifest_path = tmp_path / "run_manifest.json"
    baseline_path = tmp_path / "baseline.json"
    _write(
        manifest_path,
        _manifest(keys=["alpha_json", "beta_json", "gamma_json"], ledger_status="success"),
    )
    _write(baseline_path, _BASELINE)

    rc = run(
        [
            "--manifest",
            str(manifest_path),
            "--baseline",
            str(baseline_path),
            "--snapshot-out",
            str(tmp_path / "snapshot.json"),
            "--report-out",
            str(tmp_path / "report.json"),
        ]
    )

    assert rc == 0
    report = json.loads((tmp_path / "report.json").read_text())
    assert report["unexpected_changes"] == 0
    snapshot = json.loads((tmp_path / "snapshot.json").read_text())
    assert sorted(snapshot["artifact_files_keys"]) == ["alpha_json", "beta_json", "gamma_json"]


def test_run_returns_two_when_manifest_drifts_from_baseline(tmp_path: Path) -> None:
    manifest_path = tmp_path / "run_manifest.json"
    baseline_path = tmp_path / "baseline.json"
    _write(manifest_path, _manifest(keys=["alpha_json", "new_key_json"], ledger_status="success"))
    _write(baseline_path, _BASELINE)

    rc = run(
        [
            "--manifest",
            str(manifest_path),
            "--baseline",
            str(baseline_path),
            "--report-out",
            str(tmp_path / "report.json"),
        ]
    )

    assert rc == 2
    report = json.loads((tmp_path / "report.json").read_text())
    assert report["unexpected_changes"] > 0


def test_run_omits_snapshot_and_report_when_paths_not_given(tmp_path: Path) -> None:
    manifest_path = tmp_path / "run_manifest.json"
    baseline_path = tmp_path / "baseline.json"
    _write(
        manifest_path,
        _manifest(keys=["alpha_json", "beta_json", "gamma_json"], ledger_status="success"),
    )
    _write(baseline_path, _BASELINE)

    rc = run(["--manifest", str(manifest_path), "--baseline", str(baseline_path)])

    assert rc == 0
    assert not (tmp_path / "snapshot.json").exists()
    assert not (tmp_path / "report.json").exists()
