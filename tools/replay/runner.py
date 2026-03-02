"""CLI runner for replaying a golden corpus against a parser callable."""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from collections.abc import Callable, Mapping
from datetime import datetime
from pathlib import Path

from tools.replay.harness import (
    CorpusDocument,
    FieldExtraction,
    build_snapshot,
    run_replay,
    write_snapshot,
)

_SUPPORTED_JSON_SUFFIXES = {".json", ".jsonl", ".ndjson"}


def _parse_iso_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"invalid --generated-at value {value!r}; expected ISO-8601") from exc


def _load_corpus_rows(path: Path) -> list[Mapping[str, object]]:
    if path.suffix not in _SUPPORTED_JSON_SUFFIXES:
        raise ValueError(
            f"unsupported corpus format for '{path}'; expected one of "
            f"{', '.join(sorted(_SUPPORTED_JSON_SUFFIXES))}"
        )

    raw = path.read_text(encoding="utf-8")
    if path.suffix == ".json":
        payload = json.loads(raw)
        rows = payload.get("documents") if isinstance(payload, dict) else payload
        if not isinstance(rows, list):
            raise ValueError("JSON corpus must be a list or an object with 'documents' list")
        if not all(isinstance(row, dict) for row in rows):
            raise ValueError("JSON corpus rows must be objects")
        return rows

    rows_jsonl: list[Mapping[str, object]] = []
    for line_number, line in enumerate(raw.splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        parsed = json.loads(stripped)
        if not isinstance(parsed, dict):
            raise ValueError(f"line {line_number} must be a JSON object")
        rows_jsonl.append(parsed)
    return rows_jsonl


def _coerce_corpus_document(row: Mapping[str, object], *, index: int) -> CorpusDocument:
    document_id_raw = row.get("document_id")
    if not isinstance(document_id_raw, str) or not document_id_raw.strip():
        raise ValueError(f"corpus row {index} missing non-empty string 'document_id'")

    content_raw = row.get("content")
    if not isinstance(content_raw, str):
        raise ValueError(f"corpus row {index} missing string 'content'")

    return CorpusDocument(document_id=document_id_raw, content=content_raw)


def load_corpus(path: Path) -> list[CorpusDocument]:
    """Load golden corpus documents from JSON/JSONL."""
    rows = _load_corpus_rows(path)
    return [_coerce_corpus_document(row, index=index) for index, row in enumerate(rows, start=1)]


def _resolve_symbol(path: str) -> object:
    module_name, _, symbol = path.partition(":")
    if not module_name or not symbol:
        raise ValueError("parser path must use '<module>:<symbol>' format")
    module = importlib.import_module(module_name)
    target: object = module
    for segment in symbol.split("."):
        target = getattr(target, segment)
    return target


def load_parser(path: str) -> Callable[[CorpusDocument], Mapping[str, FieldExtraction]]:
    """Load parser callable from '<module>:<symbol>' string path."""
    parser_obj = _resolve_symbol(path)
    if not callable(parser_obj):
        raise TypeError(f"parser target '{path}' is not callable")

    def _wrapped(document: CorpusDocument) -> Mapping[str, FieldExtraction]:
        raw_fields = parser_obj(document)
        if not isinstance(raw_fields, Mapping):
            raise TypeError("parser must return a mapping of field names to payloads")
        normalized: dict[str, FieldExtraction] = {}
        for field_name, payload in raw_fields.items():
            if not isinstance(field_name, str):
                raise TypeError("parser return mapping keys must be strings")
            normalized[field_name] = _coerce_field_payload(payload)
        return normalized

    return _wrapped


def _coerce_field_payload(payload: object) -> FieldExtraction:
    if isinstance(payload, FieldExtraction):
        return payload
    if isinstance(payload, Mapping):
        if "value" not in payload:
            raise ValueError("field payload objects must include a 'value' key")
        confidence = payload.get("confidence")
        if confidence is not None and not isinstance(confidence, (int, float)):
            raise ValueError("field payload confidence must be numeric or null")
        evidence = payload.get("evidence")
        if evidence is not None and not isinstance(evidence, str):
            raise ValueError("field payload evidence must be a string or null")
        return FieldExtraction(
            value=payload["value"],
            confidence=float(confidence) if isinstance(confidence, (int, float)) else None,
            evidence=evidence,
        )
    return FieldExtraction(value=payload)


def run(argv: list[str] | None = None) -> int:
    """Execute replay runner from command-line style arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--corpus", required=True, type=Path, help="Path to golden corpus JSON/JSONL"
    )
    parser.add_argument(
        "--parser", required=True, help="Parser callable path in '<module>:<symbol>' format"
    )
    parser.add_argument(
        "--snapshot-out", required=True, type=Path, help="Output snapshot JSON path"
    )
    parser.add_argument(
        "--generated-at",
        help="Optional ISO-8601 timestamp override for deterministic snapshots",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Explicitly allow replacing existing output snapshot",
    )
    parser.add_argument(
        "--baseline-update-ticket",
        help="Ticket/PR reference required when --overwrite is used (for controlled baseline updates)",
    )
    args = parser.parse_args(argv)

    try:
        if args.overwrite and not args.baseline_update_ticket:
            raise ValueError(
                "--overwrite requires --baseline-update-ticket to enforce controlled baseline updates"
            )
        corpus = load_corpus(args.corpus)
        replay_parser = load_parser(args.parser)
        generated_at = _parse_iso_datetime(args.generated_at) if args.generated_at else None
        replay_results = run_replay(corpus, replay_parser)
        snapshot = build_snapshot(replay_results, parser_id=args.parser, generated_at=generated_at)
        write_snapshot(args.snapshot_out, snapshot, overwrite=args.overwrite)
    except (
        FileExistsError,
        FileNotFoundError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
        ImportError,
    ) as exc:
        print(f"replay-runner error: {exc}", file=sys.stderr)
        return 1

    print(
        f"Replay complete: {len(replay_results)} documents -> {args.snapshot_out}",
        file=sys.stdout,
    )
    return 0


def main() -> int:
    """Entry point."""
    return run()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
