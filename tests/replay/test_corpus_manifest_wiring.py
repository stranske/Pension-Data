"""Tests for pinned external replay-corpus manifest wiring."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from pathlib import Path

from tools.replay.corpus_manifest import load_manifest_corpus
from tools.replay.runner import run

_REPO_ROOT = Path(__file__).parents[2]


def _git(*args: str, cwd: Path) -> str:
    completed = subprocess.run(
        ["git", "-C", str(cwd), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _assert_committed_pin_matches_retained_upstream() -> None:
    config = json.loads(
        (_REPO_ROOT / "config/replay_corpus_manifest.json").read_text(encoding="utf-8")
    )
    retained = json.loads(
        (_REPO_ROOT / "tests/replay/fixtures/doc_lineage_public_corpus_pin.json").read_text(
            encoding="utf-8"
        )
    )
    assert config["upstream"]["repository"] == retained["repository"]
    assert config["upstream"]["revision"] == retained["revision"]
    assert config["upstream"]["manifest_path"] == retained["manifest_path"]
    assert config["upstream"]["manifest_schema"] == retained["manifest"]["schema_version"]
    selected = {entry["entry_id"]: entry["sha256"] for entry in config["entries"]}
    upstream = {
        entry["artifact_id"]: entry["sha256"] for entry in retained["manifest"]["artifacts"]
    }
    assert selected == upstream


def _write_manifest_fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    artifact_root = tmp_path / "doc-lineage"
    manifest_path = artifact_root / "tests/fixtures/public_corpus/manifest.json"
    pdf_path = artifact_root / "tests/fixtures/public_corpus/calpers/ic/default.pdf"
    pdf_path.parent.mkdir(parents=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(
        _REPO_ROOT / "tests/parser/fixtures/doc_lineage/calpers_fy2024_excerpt.pdf",
        pdf_path,
    )
    data = pdf_path.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": "artifact-manifest/v1",
                "artifacts": [
                    {
                        "artifact_id": "calpers-ic-default",
                        "path": "tests/fixtures/public_corpus/calpers/ic/default.pdf",
                        "sha256": digest,
                        "bytes": len(data),
                        "media_type": "application/pdf",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    _git("init", "-q", cwd=artifact_root)
    _git("add", ".", cwd=artifact_root)
    _git(
        "-c",
        "user.name=Replay Test",
        "-c",
        "user.email=replay@example.invalid",
        "commit",
        "-qm",
        "fixture",
        cwd=artifact_root,
    )
    revision = _git("rev-parse", "HEAD", cwd=artifact_root)
    config_path = tmp_path / "replay_corpus_manifest.json"
    config_path.write_text(
        json.dumps(
            {
                "schema_version": "pension-replay-corpus/v1",
                "upstream": {
                    "repository": "stranske/Doc-Lineage",
                    "revision": revision,
                    "manifest_path": "tests/fixtures/public_corpus/manifest.json",
                    "manifest_schema": "artifact-manifest/v1",
                },
                "entries": [{"entry_id": "calpers-ic-default", "sha256": digest}],
            }
        ),
        encoding="utf-8",
    )
    return config_path, artifact_root, pdf_path


def test_replay_loads_manifest_entry(tmp_path: Path) -> None:
    _assert_committed_pin_matches_retained_upstream()
    config_path, artifact_root, _ = _write_manifest_fixture(tmp_path)

    documents = load_manifest_corpus(config_path, artifact_root=artifact_root)

    assert [document.document_id for document in documents] == ["calpers-ic-default"]
    assert "Funded Ratio 76.8%" in documents[0].content

    snapshot_path = tmp_path / "snapshot.json"
    assert (
        run(
            [
                "--corpus-manifest",
                str(config_path),
                "--artifact-root",
                str(artifact_root),
                "--parser",
                "tests.replay.fixtures_parser:content_parser",
                "--snapshot-out",
                str(snapshot_path),
                "--generated-at",
                "2026-09-23T00:00:00+00:00",
            ]
        )
        == 0
    )
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    assert snapshot["documents"][0]["document_id"] == "calpers-ic-default"
    assert snapshot["documents"][0]["fields"]["contains_funded_ratio"]["value"] is True


def test_replay_manifest_rejects_changed_config_digest(tmp_path: Path) -> None:
    config_path, artifact_root, _ = _write_manifest_fixture(tmp_path)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["entries"][0]["sha256"] = "0" * 64
    config_path.write_text(json.dumps(config), encoding="utf-8")

    snapshot_path = tmp_path / "snapshot.json"
    assert (
        run(
            [
                "--corpus-manifest",
                str(config_path),
                "--artifact-root",
                str(artifact_root),
                "--parser",
                "tests.replay.fixtures_parser:content_parser",
                "--snapshot-out",
                str(snapshot_path),
            ]
        )
        == 1
    )
    assert not snapshot_path.exists()


def test_replay_manifest_rejects_changed_artifact_bytes(tmp_path: Path) -> None:
    config_path, artifact_root, pdf_path = _write_manifest_fixture(tmp_path)
    pdf_path.write_bytes(pdf_path.read_bytes() + b"changed")

    try:
        load_manifest_corpus(config_path, artifact_root=artifact_root)
    except ValueError as exc:
        assert "byte count mismatch" in str(exc)
    else:  # pragma: no cover - explicit failure message for the integrity gate
        raise AssertionError("modified artifact bytes were accepted")


def test_replay_manifest_rejects_checkout_revision_drift(tmp_path: Path) -> None:
    config_path, artifact_root, _ = _write_manifest_fixture(tmp_path)
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["upstream"]["revision"] = "f" * 40
    config_path.write_text(json.dumps(config), encoding="utf-8")

    try:
        load_manifest_corpus(config_path, artifact_root=artifact_root)
    except ValueError as exc:
        assert "artifact root revision mismatch" in str(exc)
    else:  # pragma: no cover - explicit failure message for the provenance gate
        raise AssertionError("checkout revision drift was accepted")
