#!/usr/bin/env python3
"""Build a generated web workspace bundle from one-pdf-pilot artifacts."""

from __future__ import annotations

import argparse
import importlib.util
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = ROOT / "apps" / "contracts" / "runtime-contract.json"
SMOKE_PATH = ROOT / "scripts" / "web" / "smoke_test.py"


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _load_web_smoke_test() -> Any:
    spec = importlib.util.spec_from_file_location("web_smoke_test", SMOKE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load web smoke test helper: {SMOKE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _runtime_contract_version() -> str:
    payload = _load_json(CONTRACT_PATH)
    version = payload.get("version") if isinstance(payload, dict) else None
    if not isinstance(version, str) or not version.strip():
        raise ValueError(f"runtime contract missing version: {CONTRACT_PATH}")
    return version


def _artifact_path(pilot_run_dir: Path, manifest: Mapping[str, Any], key: str) -> Path:
    artifact_files = manifest.get("artifact_files")
    if not isinstance(artifact_files, Mapping):
        raise ValueError("run_manifest.json missing artifact_files object")
    raw_path = artifact_files.get(key)
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise ValueError(f"run_manifest.json missing artifact_files.{key}")
    candidate = Path(raw_path)
    if candidate.is_absolute() or candidate.exists():
        return candidate
    root_candidate = ROOT / candidate
    if root_candidate.exists():
        return root_candidate
    return pilot_run_dir / candidate


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _as_number(value: Any, *, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _evidence_refs(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        try:
            decoded = json.loads(stripped)
        except json.JSONDecodeError:
            return [stripped]
        return _evidence_refs(decoded)
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [_as_text(item) for item in value if _as_text(item)]
    return [_as_text(value)] if _as_text(value) else []


def map_staging_row_to_ui_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Convert a staged core metric row into the static SPA row contract."""
    value = row.get("normalized_value")
    if value in (None, ""):
        value = row.get("as_reported_value")
    source_document_id = _as_text(row.get("source_document_id"))
    return {
        "confidence": _as_number(row.get("confidence")),
        "entity": _as_text(row.get("plan_id")) or "unknown-plan",
        "metric": _as_text(row.get("metric_name")),
        "metric_family": _as_text(row.get("metric_family")) or "core_metric",
        "plan_period": _as_text(row.get("plan_period")),
        "provenance": {
            "evidence_refs": _evidence_refs(row.get("evidence_refs")),
            "source_document": source_document_id,
        },
        "value": value,
    }


def _load_staging_rows(path: Path) -> list[Mapping[str, Any]]:
    payload = _load_json(path)
    rows: Any
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, Mapping):
        rows = payload.get("rows") or payload.get("staging_core_metrics_rows")
    else:
        rows = None
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"staging core metrics file has no rows: {path}")
    normalized: list[Mapping[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise ValueError(f"staging row {index} is not an object in {path}")
        normalized.append(row)
    return normalized


def _last_updated(manifest: Mapping[str, Any]) -> str:
    input_payload = manifest.get("input")
    if isinstance(input_payload, Mapping):
        for key in ("effective_date", "ingestion_date", "fetched_at"):
            value = _as_text(input_payload.get(key))
            if value:
                return value
    return datetime.now(UTC).date().isoformat()


def build_workspace_bundle(pilot_run_dir: Path) -> dict[str, Any]:
    pilot_run_dir = pilot_run_dir.resolve()
    manifest_path = pilot_run_dir / "run_manifest.json"
    manifest = _load_json(manifest_path)
    if not isinstance(manifest, Mapping):
        raise ValueError(f"run manifest must be an object: {manifest_path}")

    staging_rows = _load_staging_rows(
        _artifact_path(pilot_run_dir, manifest, "staging_core_metrics_json")
    )
    rows = [map_staging_row_to_ui_row(row) for row in staging_rows]
    rows.sort(
        key=lambda row: (
            _as_text(row["entity"]),
            _as_text(row["plan_period"]),
            _as_text(row["metric_family"]),
            _as_text(row["metric"]),
        )
    )
    run_id = _as_text(manifest.get("run_id")) or pilot_run_dir.name
    bundle = {
        "contractVersion": _runtime_contract_version(),
        "data_origin": "generated",
        "datasets": [
            {
                "domain": "pension",
                "freshness": "generated",
                "id": f"one-pdf-pilot-{run_id}",
                "kind": "core_metrics",
                "lastUpdated": _last_updated(manifest),
                "name": f"One-PDF Pilot Generated Metrics ({run_id})",
                "rows": rows,
            }
        ],
    }
    smoke = _load_web_smoke_test()
    smoke._assert_workspace_bundle(
        bundle,
        path_label="generated workspace bundle",
        reject_fixture=True,
    )
    return bundle


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pilot-run-dir",
        type=Path,
        required=True,
        help="Directory containing run_manifest.json from one-pdf-pilot.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Path for the generated workspace bundle JSON.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    bundle = build_workspace_bundle(args.pilot_run_dir)
    _write_json(args.out, bundle)
    print(f"wrote generated workspace bundle: {args.out}")


if __name__ == "__main__":
    main()
