"""Golden reference run for the NL->SQL query tool (deterministic, zero-egress).

This driver exercises ``run_nl_sql_chain`` over a committed corpus using a
deterministic in-process **stub chain** (never a live LLM/network provider) and
the production ``default_nl_query_policy``. It enforces two invariants the repo's
NL tool depends on and that have no other reference gate:

* every returned row carries non-null ``source_document_id`` provenance, and
* a generated ``SELECT`` that omits ``source_document_id`` is rejected with
  ``error.code == "UNSAFE_SQL"`` (the guard at
  ``src/pension_data/langchain/nl_sql_chain.py``).

It emits a deterministic snapshot and a diff report (schema-compatible with
``tools/ci_quality/replay_gate.py``) so drift is gated at ``--max-unexpected 0``,
and it seeds a SQLite fixture + operation log so ``scripts/langchain/nl_replay.py``
can replay a recorded request in CI.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections.abc import Mapping
from pathlib import Path
from typing import Any, TypedDict

from pension_data.langchain.nl_sql_chain import NLToSQLRequest, run_nl_sql_chain
from pension_data.langchain.observability import (
    append_nl_operation_log,
    build_nl_operation_log_entry,
    load_nl_operation_logs,
    replay_logged_request,
)
from pension_data.query.sql_safety import default_nl_query_policy


class _StubChain:
    """Deterministic in-process chain returning a fixed SQL string (no network)."""

    def __init__(self, sql: str) -> None:
        self._sql = sql

    def invoke(self, values: Mapping[str, Any]) -> str:
        del values
        return self._sql


class QuerySnapshot(TypedDict):
    """Deterministic per-query golden snapshot row."""

    id: str
    status: str
    error_code: str | None
    returned_rows: int
    provenance_complete: bool


def _seed_connection(connection: sqlite3.Connection, corpus: dict[str, Any]) -> None:
    table = str(corpus["table"])
    connection.execute(str(corpus["schema"]))
    for row in corpus["rows"]:
        columns = list(row.keys())
        placeholders = ", ".join(["?"] * len(columns))
        connection.execute(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
            [row[column] for column in columns],
        )
    connection.commit()


def _provenance_complete(returned_rows: int, provenance: tuple[Any, ...]) -> bool:
    if len(provenance) != returned_rows:
        return False
    return all(row.source_document_id is not None for row in provenance)


def run_corpus(corpus: dict[str, Any]) -> tuple[list[QuerySnapshot], list[str]]:
    """Run every corpus query through the chain and return snapshot + violations."""
    policy = default_nl_query_policy()
    snapshot: list[QuerySnapshot] = []
    violations: list[str] = []

    for query in corpus["queries"]:
        connection = sqlite3.connect(":memory:")
        try:
            _seed_connection(connection, corpus)
            response = run_nl_sql_chain(
                connection=connection,
                request=NLToSQLRequest(question=str(query["question"])),
                chain=_StubChain(str(query["sql"])),
                policy=policy,
            )
        finally:
            connection.close()

        error_code = response.error.code if response.error is not None else None
        complete = _provenance_complete(response.metadata.returned_rows, response.provenance)
        snapshot.append(
            {
                "id": str(query["id"]),
                "status": response.status,
                "error_code": error_code,
                "returned_rows": response.metadata.returned_rows,
                "provenance_complete": complete,
            }
        )

        if query.get("expect_unsafe"):
            if response.status != "error" or error_code != "UNSAFE_SQL":
                violations.append(
                    f"query '{query['id']}' expected UNSAFE_SQL rejection but got "
                    f"status={response.status} code={error_code}"
                )
        else:
            if response.status != "ok":
                violations.append(
                    f"query '{query['id']}' expected status ok but got {response.status} "
                    f"(error={error_code})"
                )
            elif response.metadata.returned_rows > 0 and not complete:
                violations.append(
                    f"query '{query['id']}' returned rows without complete source_document_id "
                    "provenance"
                )

    snapshot.sort(key=lambda item: item["id"])
    return snapshot, violations


def diff_snapshot(*, baseline: dict[str, Any], snapshot: list[QuerySnapshot]) -> dict[str, Any]:
    """Diff the golden snapshot against the committed baseline (replay-gate schema)."""
    baseline_rows = {str(row["id"]): row for row in baseline.get("queries", [])}
    current_rows = {row["id"]: row for row in snapshot}
    changes: list[dict[str, Any]] = []

    for query_id in sorted(set(baseline_rows) | set(current_rows)):
        base_row = baseline_rows.get(query_id)
        cur_row = current_rows.get(query_id)
        if base_row is None or cur_row is None:
            changes.append(
                {
                    "field": query_id,
                    "attribute": "query_presence",
                    "baseline": base_row is not None,
                    "current": cur_row is not None,
                    "classification": "unexpected_drift",
                }
            )
            continue
        for attribute in ("status", "error_code", "returned_rows", "provenance_complete"):
            if base_row.get(attribute) != cur_row.get(attribute):
                changes.append(
                    {
                        "field": query_id,
                        "attribute": attribute,
                        "baseline": base_row.get(attribute),
                        "current": cur_row.get(attribute),
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


def _seed_replay_fixture(corpus: dict[str, Any], emit_root: Path) -> dict[str, Any]:
    """Seed a SQLite fixture + operation log so nl_replay.py can replay in CI."""
    replay_query = next(
        query for query in corpus["queries"] if query["id"] == corpus["replay_query_id"]
    )

    db_path = emit_root / "seed.db"
    if db_path.exists():
        db_path.unlink()
    file_conn = sqlite3.connect(db_path)
    try:
        _seed_connection(file_conn, corpus)
    finally:
        file_conn.close()

    # Run the request once (against an equivalent in-memory copy) to record a log row.
    record_conn = sqlite3.connect(":memory:")
    try:
        _seed_connection(record_conn, corpus)
        request = NLToSQLRequest(question=str(replay_query["question"]))
        response = run_nl_sql_chain(
            connection=record_conn,
            request=request,
            chain=_StubChain(str(replay_query["sql"])),
            policy=default_nl_query_policy(),
        )
    finally:
        record_conn.close()

    log_path = emit_root / "nl_operations.jsonl"
    if log_path.exists():
        log_path.unlink()
    entry = build_nl_operation_log_entry(
        request=request,
        response=response,
        provider="stub",
        model="golden-reference",
        correlation_id="nl-golden",
    )
    append_nl_operation_log(path=log_path, entry=entry)

    expected = {
        "status": response.status,
        "returned_rows": response.metadata.returned_rows,
        "request_id": response.metadata.request_id,
    }
    _write_json(emit_root / "replay_expected.json", expected)

    # Confirm the recorded request replays deterministically against the fixture DB.
    replay_conn = sqlite3.connect(db_path)
    try:
        recorded = load_nl_operation_logs(log_path)[-1]
        replayed = replay_logged_request(entry=recorded, connection=replay_conn)
    finally:
        replay_conn.close()
    if (
        replayed.status != response.status
        or replayed.metadata.returned_rows != response.metadata.returned_rows
    ):
        raise SystemExit(
            "nl-sql golden: replay self-check mismatch "
            f"({replayed.status}/{replayed.metadata.returned_rows} != "
            f"{response.status}/{response.metadata.returned_rows})"
        )
    return expected


def run(argv: list[str] | None = None) -> int:
    """Execute the NL->SQL golden run and emit snapshot/diff/replay artifacts."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", required=True, type=Path, help="Committed NL corpus JSON")
    parser.add_argument("--baseline", required=True, type=Path, help="Committed snapshot baseline")
    parser.add_argument(
        "--emit-root",
        required=True,
        type=Path,
        help="Output root for snapshot, diff report, and replay fixture artifacts",
    )
    args = parser.parse_args(argv)

    corpus = json.loads(args.corpus.read_text(encoding="utf-8"))
    snapshot, violations = run_corpus(corpus)

    args.emit_root.mkdir(parents=True, exist_ok=True)
    _write_json(args.emit_root / "snapshot.json", {"queries": snapshot})

    if violations:
        for violation in violations:
            print(f"nl-sql golden violation: {violation}")
        return 1

    baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    report = diff_snapshot(baseline=baseline, snapshot=snapshot)
    _write_json(args.emit_root / "diff_report.json", report)

    _seed_replay_fixture(corpus, args.emit_root)

    print(
        "NL->SQL golden complete: "
        f"{len(snapshot)} queries, {report['unexpected_changes']} unexpected drift"
    )
    return 2 if report["unexpected_changes"] > 0 else 0


def main() -> int:
    """Entry point."""
    return run()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
