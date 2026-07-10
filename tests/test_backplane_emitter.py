"""Compatibility tests for the issue-documented backplane emitter path."""

from __future__ import annotations

import copy
import json

from tests.ops.test_backplane_emitter import _load, _validate, valid_reference_run


def test_manifest_must_cover_all_artifacts(tmp_path):
    paths = valid_reference_run(tmp_path)
    run_payload = _load(paths["run_json"])
    manifest = _load(paths["manifest"])
    missing_id = run_payload["outputs"]["artifact_ids"][0]
    original_manifest_ids = {artifact["artifact_id"] for artifact in manifest["artifacts"]}
    assert missing_id in original_manifest_ids, "artifact missing from manifest"

    broken = copy.deepcopy(manifest)
    broken["artifacts"] = [
        artifact for artifact in broken["artifacts"] if artifact["artifact_id"] != missing_id
    ]
    paths["manifest"].write_text(json.dumps(broken, indent=2), encoding="utf-8")

    manifest_ids = {artifact["artifact_id"] for artifact in broken["artifacts"]}
    assert missing_id in set(run_payload["outputs"]["artifact_ids"])
    assert missing_id not in manifest_ids, "artifact missing from manifest"

    completed = _validate(paths["run_json"], paths["manifest"])
    assert completed.returncode != 0
    assert f"artifact_id '{missing_id}' not in manifest" in completed.stderr
