"""Diff a one-PDF pilot ``run_manifest.json`` against a committed golden baseline.

The pilot manifest embeds run-specific absolute paths (output root + ``run_id``),
so only the stable subset is compared: the **set of ``artifact_files`` keys** and
the ``ledger_status`` value. Any added/removed artifact key or a changed ledger
status is emitted as ``unexpected_drift`` in a diff report whose schema matches
``tools/ci_quality/replay_gate.py`` so the same ``--max-unexpected 0`` gate step
used by ``extraction-golden-regression.yml`` rejects the drift.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import TypedDict


class ManifestSnapshot(TypedDict):
    """Stable, diffable subset of a pilot run manifest."""

    artifact_files_keys: list[str]
    ledger_status: object


class ManifestDiffChange(TypedDict):
    """Single drift entry compatible with the replay gate schema."""

    field: str
    attribute: str
    baseline: object
    current: object
    classification: str


class ManifestDiffReport(TypedDict):
    """Replay-gate-compatible diff report."""

    total_changes: int
    expected_changes: int
    unexpected_changes: int
    changes: list[ManifestDiffChange]


def extract_snapshot(manifest: dict[str, object]) -> ManifestSnapshot:
    """Extract the stable diffable subset from a live pilot manifest."""
    artifact_files = manifest.get("artifact_files")
    if not isinstance(artifact_files, dict):
        raise ValueError("manifest.artifact_files must be an object")
    if "ledger_status" not in manifest:
        raise ValueError("manifest missing required key 'ledger_status'")
    return {
        "artifact_files_keys": sorted(str(key) for key in artifact_files),
        "ledger_status": manifest["ledger_status"],
    }


def _load_baseline(baseline: dict[str, object]) -> ManifestSnapshot:
    keys = baseline.get("artifact_files_keys")
    if not isinstance(keys, list) or not all(isinstance(item, str) for item in keys):
        raise ValueError("baseline.artifact_files_keys must be a list of strings")
    if "ledger_status" not in baseline:
        raise ValueError("baseline missing required key 'ledger_status'")
    return {
        "artifact_files_keys": sorted(keys),
        "ledger_status": baseline["ledger_status"],
    }


def diff_manifest(
    *, baseline: dict[str, object], current_manifest: dict[str, object]
) -> ManifestDiffReport:
    """Compare the stable subset of a live manifest against a golden baseline."""
    base = _load_baseline(baseline)
    snapshot = extract_snapshot(current_manifest)

    changes: list[ManifestDiffChange] = []

    base_keys = set(base["artifact_files_keys"])
    current_keys = set(snapshot["artifact_files_keys"])
    for key in sorted(base_keys - current_keys):
        changes.append(
            {
                "field": key,
                "attribute": "artifact_files_key_presence",
                "baseline": True,
                "current": False,
                "classification": "unexpected_drift",
            }
        )
    for key in sorted(current_keys - base_keys):
        changes.append(
            {
                "field": key,
                "attribute": "artifact_files_key_presence",
                "baseline": False,
                "current": True,
                "classification": "unexpected_drift",
            }
        )

    if base["ledger_status"] != snapshot["ledger_status"]:
        changes.append(
            {
                "field": "ledger_status",
                "attribute": "value",
                "baseline": base["ledger_status"],
                "current": snapshot["ledger_status"],
                "classification": "unexpected_drift",
            }
        )

    return {
        "total_changes": len(changes),
        "expected_changes": 0,
        "unexpected_changes": len(changes),
        "changes": changes,
    }


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run(argv: list[str] | None = None) -> int:
    """Build snapshot + diff report from a live manifest and committed baseline."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest", required=True, type=Path, help="Path to live run_manifest.json"
    )
    parser.add_argument(
        "--baseline", required=True, type=Path, help="Path to committed golden baseline JSON"
    )
    parser.add_argument(
        "--snapshot-out", type=Path, help="Optional path for the extracted stable snapshot"
    )
    parser.add_argument(
        "--report-out", type=Path, help="Optional path for the replay-gate-compatible diff report"
    )
    args = parser.parse_args(argv)

    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or not isinstance(baseline, dict):
        print("manifest-gate error: manifest and baseline must be JSON objects")
        return 1

    snapshot = extract_snapshot(manifest)
    report = diff_manifest(baseline=baseline, current_manifest=manifest)

    if args.snapshot_out is not None:
        _write_json(args.snapshot_out, snapshot)
    if args.report_out is not None:
        _write_json(args.report_out, report)

    print(
        "Manifest diff complete: "
        f"{report['total_changes']} changes "
        f"({report['unexpected_changes']} unexpected)"
    )
    return 2 if report["unexpected_changes"] > 0 else 0


def main() -> int:
    """Entry point."""
    return run()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
