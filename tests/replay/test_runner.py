"""Tests for the replay CLI runner."""

from __future__ import annotations

import json
from pathlib import Path

from tools.replay.runner import run


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    body = "\n".join(json.dumps(row) for row in rows)
    path.write_text(body + "\n", encoding="utf-8")


def test_runner_generates_deterministic_snapshot_for_corpus_order(tmp_path: Path) -> None:
    corpus_path = tmp_path / "golden.jsonl"
    _write_jsonl(
        corpus_path,
        [
            {"document_id": "doc-b", "content": "beta"},
            {"document_id": "doc-a", "content": "alpha"},
        ],
    )
    parser_path = "tests.replay.fixtures_parser:parser"
    generated_at = "2026-03-02T00:00:00+00:00"

    first_snapshot = tmp_path / "snapshot_a.json"
    second_snapshot = tmp_path / "snapshot_b.json"

    first_exit = run(
        [
            "--corpus",
            str(corpus_path),
            "--parser",
            parser_path,
            "--snapshot-out",
            str(first_snapshot),
            "--generated-at",
            generated_at,
        ]
    )
    assert first_exit == 0

    _write_jsonl(
        corpus_path,
        [
            {"document_id": "doc-a", "content": "alpha"},
            {"document_id": "doc-b", "content": "beta"},
        ],
    )

    second_exit = run(
        [
            "--corpus",
            str(corpus_path),
            "--parser",
            parser_path,
            "--snapshot-out",
            str(second_snapshot),
            "--generated-at",
            generated_at,
        ]
    )
    assert second_exit == 0

    first_payload = json.loads(first_snapshot.read_text(encoding="utf-8"))
    second_payload = json.loads(second_snapshot.read_text(encoding="utf-8"))
    assert first_payload == second_payload
    assert first_payload["artifact_type"] == "pension_replay_baseline"
    assert first_payload["schema_version"] == 1
    assert first_payload["baseline_version"] == "v1"
    assert first_payload["parser_id"] == parser_path
    assert [row["document_id"] for row in first_payload["documents"]] == ["doc-a", "doc-b"]


def test_runner_requires_overwrite_for_existing_snapshot(tmp_path: Path) -> None:
    corpus_path = tmp_path / "golden.json"
    corpus_path.write_text(
        json.dumps({"documents": [{"document_id": "doc-a", "content": "alpha"}]}),
        encoding="utf-8",
    )
    snapshot_path = tmp_path / "snapshot.json"
    args = [
        "--corpus",
        str(corpus_path),
        "--parser",
        "tests.replay.fixtures_parser:parser",
        "--snapshot-out",
        str(snapshot_path),
        "--generated-at",
        "2026-03-02T00:00:00+00:00",
    ]

    assert run(args) == 0
    assert run(args) == 1
    assert run([*args, "--overwrite"]) == 1
    assert run([*args, "--overwrite", "--baseline-update-ticket", "#44"]) == 0
