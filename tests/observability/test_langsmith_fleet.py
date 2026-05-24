"""Tests for Pension-Data LangSmith fleet artifact records."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from pension_data.langchain.nl_sql_chain import (
    InMemoryLangSmithTraceSink,
    NLToSQLRequest,
    run_nl_sql_chain,
)
from pension_data.observability import langsmith_fleet
from pension_data.query.sql_safety import SQLSafetyPolicy


def _seed_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.execute(
        "CREATE TABLE sample_metrics ("
        "id INTEGER PRIMARY KEY, "
        "metric TEXT NOT NULL, "
        "value REAL NOT NULL, "
        "source_document_id TEXT NOT NULL, "
        "evidence_refs TEXT, "
        "confidence REAL)"
    )
    connection.executemany(
        "INSERT INTO sample_metrics "
        "(id, metric, value, source_document_id, evidence_refs, confidence) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, "funded_ratio", 0.79, "doc:1", '["p.10"]', 0.9),
            (2, "funded_ratio", 0.81, "doc:1", '["p.11"]', 0.85),
            (3, "discount_rate", 0.0675, "doc:2", '["p.20"]', 0.8),
        ],
    )
    return connection


def _policy() -> SQLSafetyPolicy:
    return SQLSafetyPolicy(
        allowed_relations=("sample_metrics",),
        allowed_columns=(
            "id",
            "metric",
            "value",
            "source_document_id",
            "evidence_refs",
            "confidence",
        ),
        banned_clauses=("pragma", "into outfile", "copy ", "pg_catalog", "information_schema"),
        max_rows=500,
        max_timeout_ms=2_000,
        require_source_document_id=False,
    )


class _StaticChain:
    def __init__(self, sql: str) -> None:
        self._sql = sql

    def invoke(self, values: Mapping[str, Any]) -> str:
        del values
        return self._sql


def test_build_fleet_records_uses_no_secret_status_when_key_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(langsmith_fleet.ENV_LANGSMITH_KEY, raising=False)
    context = langsmith_fleet.FleetRunContext(
        run_id="nlq:abc",
        query_category="funded_ratio_lookup",
    )

    records = langsmith_fleet.build_fleet_records(
        context=context,
        sql_validation_status="pass",
        read_only_status="read_only",
        row_count=2,
        max_rows=10,
    )

    operations = [record["operation"] for record in records]
    assert operations == ["sql-generation", "validation", "execution", "replay"]
    assert {record["status"] for record in records[:3]} == {"no_secret"}
    assert records[3]["status"] == "skipped"
    for record in records:
        assert record["schema_version"] == langsmith_fleet.SCHEMA_VERSION
        assert record["repo"] == "stranske/Pension-Data"
        assert record["surface"] == "nl-to-sql"
        assert record["github_issue"] == "stranske/Pension-Data#445"
        assert record["domain"]["query_category"] == "funded_ratio_lookup"
        assert record["domain"]["sql_validation_status"] == "pass"
        assert record["domain"]["read_only_status"] == "read_only"
        assert record["domain"]["row_count"] == 2
        assert record["domain"]["max_rows"] == 10
        serialized = json.dumps(record)
        assert "SELECT" not in serialized
        assert "funded_ratio" not in serialized.lower().split(":")[0]


def test_build_fleet_records_enables_langsmith_defaults_when_key_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(langsmith_fleet.ENV_LANGSMITH_KEY, "test-key")
    monkeypatch.delenv(langsmith_fleet.ENV_LANGCHAIN_TRACING_V2, raising=False)
    monkeypatch.delenv(langsmith_fleet.ENV_LANGCHAIN_API_KEY, raising=False)
    monkeypatch.delenv(langsmith_fleet.ENV_LANGCHAIN_PROJECT, raising=False)
    monkeypatch.delenv(langsmith_fleet.ENV_LANGSMITH_PROJECT, raising=False)
    context = langsmith_fleet.FleetRunContext(
        run_id="nlq:def",
        query_category="discount_rate_lookup",
        provider="openai",
        model="gpt-4o",
        trace_id="trace-123",
        trace_url="https://smith.langchain.com/r/trace-123",
    )

    records = langsmith_fleet.build_fleet_records(
        context=context,
        sql_validation_status="pass",
        read_only_status="read_only",
        row_count=1,
        replay_dataset_id="ds:funded_ratio",
        replay_run_id="run:001",
        replay_match_status="match",
    )

    statuses = [record["status"] for record in records]
    assert statuses == ["success", "success", "success", "success"]
    assert records[0]["trace_id"] == "trace-123"
    assert records[0]["trace_url"] == "https://smith.langchain.com/r/trace-123"
    assert records[0]["provider"] == "openai"
    assert records[0]["model"] == "gpt-4o"
    assert records[3]["domain"]["replay_dataset_id"] == "ds:funded_ratio"
    assert records[3]["domain"]["replay_run_id"] == "run:001"
    assert records[3]["domain"]["replay_match_status"] == "match"
    import os

    assert os.environ[langsmith_fleet.ENV_LANGCHAIN_PROJECT] == langsmith_fleet.DEFAULT_PROJECT
    assert os.environ[langsmith_fleet.ENV_LANGSMITH_PROJECT] == langsmith_fleet.DEFAULT_PROJECT
    assert os.environ[langsmith_fleet.ENV_LANGCHAIN_TRACING_V2] == "true"
    assert os.environ[langsmith_fleet.ENV_LANGCHAIN_API_KEY] == "test-key"


def test_build_fleet_records_marks_validation_failure_and_skips_later_stages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(langsmith_fleet.ENV_LANGSMITH_KEY, raising=False)
    context = langsmith_fleet.FleetRunContext(
        run_id="nlq:unsafe",
        query_category="schema_inspection",
    )

    records = langsmith_fleet.build_fleet_records(
        context=context,
        sql_validation_status="unsafe",
        read_only_status="blocked",
        row_count=0,
        error_code="UNSAFE_SQL",
    )

    statuses = {record["operation"]: record["status"] for record in records}
    assert statuses == {
        "sql-generation": "no_secret",
        "validation": "error",
        "execution": "skipped",
        "replay": "skipped",
    }
    validation = next(record for record in records if record["operation"] == "validation")
    assert validation["error_category"] == "UNSAFE_SQL"
    assert validation["domain"]["sql_validation_status"] == "unsafe"
    assert validation["domain"]["read_only_status"] == "blocked"
    assert validation["domain"]["row_count"] == 0


def test_build_fleet_records_marks_execution_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(langsmith_fleet.ENV_LANGSMITH_KEY, raising=False)
    context = langsmith_fleet.FleetRunContext(
        run_id="nlq:timeout",
        query_category="latency_audit",
    )

    records = langsmith_fleet.build_fleet_records(
        context=context,
        sql_validation_status="pass",
        read_only_status="read_only",
        row_count=0,
        error_code="TIMEOUT",
    )

    statuses = {record["operation"]: record["status"] for record in records}
    assert statuses == {
        "sql-generation": "no_secret",
        "validation": "no_secret",
        "execution": "error",
        "replay": "skipped",
    }
    execution = next(record for record in records if record["operation"] == "execution")
    assert execution["error_category"] == "TIMEOUT"


def test_build_fleet_records_from_response_round_trip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(langsmith_fleet.ENV_LANGSMITH_KEY, raising=False)
    connection = _seed_connection()
    traces = InMemoryLangSmithTraceSink(events=[])
    try:
        request = NLToSQLRequest(
            question="Show funded ratio values by id",
            max_rows=10,
        )
        response = run_nl_sql_chain(
            connection=connection,
            request=request,
            chain=_StaticChain(
                "SELECT id, value FROM sample_metrics WHERE metric = 'funded_ratio'"
            ),
            trace_sink=traces,
            policy=_policy(),
        )
    finally:
        connection.close()

    context = langsmith_fleet.FleetRunContext(
        run_id=response.metadata.request_id,
        query_category="funded_ratio_lookup",
    )
    records = langsmith_fleet.build_fleet_records_from_response(
        context=context,
        response=response,
        request=request,
    )

    assert response.status == "ok"
    statuses = {record["operation"]: record["status"] for record in records}
    assert statuses == {
        "sql-generation": "no_secret",
        "validation": "no_secret",
        "execution": "no_secret",
        "replay": "skipped",
    }
    for record in records:
        assert record["domain"]["sql_validation_status"] == "pass"
        assert record["domain"]["read_only_status"] == "read_only"
        assert record["domain"]["row_count"] == 2
        assert record["domain"]["max_rows"] == 10
        assert record["domain"]["trace_event_count"] == len(traces.events)
        assert record["domain"]["latency_ms"] >= 0
        serialized = json.dumps(record)
        assert "DELETE" not in serialized
        assert response.sql is not None and response.sql not in serialized


def test_build_fleet_records_from_response_unsafe_sql_blocked() -> None:
    connection = _seed_connection()
    try:
        request = NLToSQLRequest(question="Delete all funded rows now")
        response = run_nl_sql_chain(
            connection=connection,
            request=request,
            chain=_StaticChain("DELETE FROM sample_metrics"),
            trace_sink=None,
            policy=_policy(),
        )
    finally:
        connection.close()

    context = langsmith_fleet.FleetRunContext(
        run_id=response.metadata.request_id,
        query_category="destructive_attempt",
    )
    records = langsmith_fleet.build_fleet_records_from_response(
        context=context,
        response=response,
        request=request,
    )

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "UNSAFE_SQL"
    statuses = {record["operation"]: record["status"] for record in records}
    assert statuses["validation"] == "error"
    assert statuses["execution"] == "skipped"
    for record in records:
        assert record["domain"]["sql_validation_status"] == "unsafe"
        assert record["domain"]["read_only_status"] == "blocked"
        assert record["domain"]["row_count"] == 0


def test_write_fleet_records_emits_deterministic_ndjson(tmp_path: Path) -> None:
    path = tmp_path / langsmith_fleet.ARTIFACT_NAME
    records = [
        {
            "schema_version": langsmith_fleet.SCHEMA_VERSION,
            "repo": "stranske/Pension-Data",
            "surface": "nl-to-sql",
            "operation": "execution",
            "run_id": "nlq:abc",
            "status": "no_secret",
            "github_issue": "stranske/Pension-Data#445",
            "recorded_at": "2026-05-23T00:00:00Z",
            "domain": {
                "query_category": "funded_ratio_lookup",
                "sql_validation_status": "pass",
                "read_only_status": "read_only",
                "row_count": 2,
                "max_rows": 10,
                "stage": "execution",
            },
        }
    ]

    result_path = langsmith_fleet.write_fleet_records(path, records)

    assert result_path == path
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    parsed = json.loads(lines[0])
    assert parsed == records[0]
    # NDJSON output must be sorted-key and trailing newline terminated for
    # reproducible diffs across re-runs of the same NL request.
    assert lines[0].startswith('{"domain":')
    assert path.read_text(encoding="utf-8").endswith("\n")


def test_write_fleet_records_empty_input_writes_empty_file(tmp_path: Path) -> None:
    path = tmp_path / langsmith_fleet.ARTIFACT_NAME
    langsmith_fleet.write_fleet_records(path, [])
    assert path.read_text(encoding="utf-8") == ""


def test_append_fleet_records_accumulates_lines(tmp_path: Path) -> None:
    path = tmp_path / langsmith_fleet.ARTIFACT_NAME
    base_record = {
        "schema_version": langsmith_fleet.SCHEMA_VERSION,
        "repo": "stranske/Pension-Data",
        "surface": "nl-to-sql",
        "operation": "execution",
        "run_id": "nlq:1",
        "status": "no_secret",
        "github_issue": "stranske/Pension-Data#445",
        "recorded_at": "2026-05-23T00:00:00Z",
        "domain": {
            "query_category": "funded_ratio_lookup",
            "sql_validation_status": "pass",
            "read_only_status": "read_only",
            "row_count": 1,
            "stage": "execution",
        },
    }
    langsmith_fleet.append_fleet_records(path, [base_record])
    langsmith_fleet.append_fleet_records(
        path,
        [{**base_record, "run_id": "nlq:2"}],
    )
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["run_id"] == "nlq:1"
    assert json.loads(lines[1])["run_id"] == "nlq:2"


def test_run_nl_query_endpoint_emits_fleet_artifact_when_category_set(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(langsmith_fleet.ENV_LANGSMITH_KEY, raising=False)
    from pension_data.api.auth import SCOPE_NL, APIKeyStore
    from pension_data.api.routes.nl import run_nl_query_endpoint

    key_store = APIKeyStore()
    secret, _ = key_store.create_key(scopes=(SCOPE_NL,))
    connection = _seed_connection()
    log_path = tmp_path / "nl_operations.jsonl"
    fleet_path = tmp_path / "langsmith" / langsmith_fleet.ARTIFACT_NAME
    try:
        result = run_nl_query_endpoint(
            api_key_header=secret,
            key_store=key_store,
            connection=connection,
            request=NLToSQLRequest(
                question="Show funded ratio values by id",
                max_rows=10,
            ),
            chain=_StaticChain(
                "SELECT id, value FROM sample_metrics WHERE metric = 'funded_ratio'"
            ),
            provider="openai",
            model="gpt-4o-mini",
            log_path=log_path,
            policy=_policy(),
            query_category="funded_ratio_lookup",
            fleet_artifact_path=fleet_path,
            fleet_trace_id="trace-xyz",
            fleet_trace_url="https://smith.langchain.com/r/trace-xyz",
            fleet_github_pr="stranske/Pension-Data#999",
        )
    finally:
        connection.close()

    assert result.response.status == "ok"
    lines = fleet_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 4
    records = [json.loads(line) for line in lines]
    assert [record["operation"] for record in records] == [
        "sql-generation",
        "validation",
        "execution",
        "replay",
    ]
    for record in records[:3]:
        assert record["status"] == "no_secret"
        assert record["domain"]["query_category"] == "funded_ratio_lookup"
        assert record["domain"]["sql_validation_status"] == "pass"
        assert record["domain"]["row_count"] == 2
        assert record["trace_id"] == "trace-xyz"
        assert record["trace_url"] == "https://smith.langchain.com/r/trace-xyz"
        assert record["github_pr"] == "stranske/Pension-Data#999"
    assert records[3]["status"] == "skipped"
    assert result.audit_event["langsmith_query_category"] == "funded_ratio_lookup"
    assert result.audit_event["langsmith_trace_id"] == "trace-xyz"


def test_run_nl_query_endpoint_skips_fleet_artifact_when_no_category(
    tmp_path: Path,
) -> None:
    from pension_data.api.auth import SCOPE_NL, APIKeyStore
    from pension_data.api.routes.nl import run_nl_query_endpoint

    key_store = APIKeyStore()
    secret, _ = key_store.create_key(scopes=(SCOPE_NL,))
    connection = _seed_connection()
    log_path = tmp_path / "nl_operations.jsonl"
    fleet_path = tmp_path / "langsmith" / langsmith_fleet.ARTIFACT_NAME
    try:
        run_nl_query_endpoint(
            api_key_header=secret,
            key_store=key_store,
            connection=connection,
            request=NLToSQLRequest(
                question="Show funded ratio values by id",
                max_rows=10,
            ),
            chain=_StaticChain(
                "SELECT id, value FROM sample_metrics WHERE metric = 'funded_ratio'"
            ),
            log_path=log_path,
            policy=_policy(),
        )
    finally:
        connection.close()

    assert not fleet_path.exists()


def test_append_fleet_records_trims_to_retention_limit(tmp_path: Path) -> None:
    path = tmp_path / langsmith_fleet.ARTIFACT_NAME
    records = [
        {
            "schema_version": langsmith_fleet.SCHEMA_VERSION,
            "repo": "stranske/Pension-Data",
            "surface": "nl-to-sql",
            "operation": "execution",
            "run_id": f"nlq:{index}",
            "status": "no_secret",
            "github_issue": "stranske/Pension-Data#445",
            "recorded_at": "2026-05-23T00:00:00Z",
            "domain": {
                "query_category": "funded_ratio_lookup",
                "sql_validation_status": "pass",
                "read_only_status": "read_only",
                "row_count": 1,
                "stage": "execution",
            },
        }
        for index in range(5)
    ]
    langsmith_fleet.append_fleet_records(path, records, retention_limit=3)
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3
    assert json.loads(lines[0])["run_id"] == "nlq:2"
    assert json.loads(lines[-1])["run_id"] == "nlq:4"


def test_fleet_artifact_never_contains_raw_question_sql_or_row_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Task 5: no raw prompts, generated SQL, or row payloads in the artifact."""
    monkeypatch.delenv(langsmith_fleet.ENV_LANGSMITH_KEY, raising=False)
    from pension_data.api.auth import SCOPE_NL, APIKeyStore
    from pension_data.api.routes.nl import run_nl_query_endpoint

    question = "UNIQUE_QUESTION_SENTINEL_XYZ: what is the funded ratio for plan alpha?"
    raw_sql = "SELECT id, value FROM sample_metrics WHERE metric = 'funded_ratio'"
    raw_row_value = "0.79"  # A value that appears in seeded row data

    key_store = APIKeyStore()
    secret, _ = key_store.create_key(scopes=(SCOPE_NL,))
    connection = _seed_connection()
    log_path = tmp_path / "nl_operations.jsonl"
    fleet_path = tmp_path / "langsmith" / langsmith_fleet.ARTIFACT_NAME
    try:
        result = run_nl_query_endpoint(
            api_key_header=secret,
            key_store=key_store,
            connection=connection,
            request=NLToSQLRequest(question=question, max_rows=10),
            chain=_StaticChain(raw_sql),
            log_path=log_path,
            policy=_policy(),
            query_category="funded_ratio_lookup",
            fleet_artifact_path=fleet_path,
        )
    finally:
        connection.close()

    assert result.response.status == "ok"
    artifact_text = fleet_path.read_text(encoding="utf-8")
    assert question not in artifact_text, "raw NL question must not appear in fleet artifact"
    assert raw_sql not in artifact_text, "raw generated SQL must not appear in fleet artifact"
    assert raw_row_value not in artifact_text, "raw row values must not appear in fleet artifact"


def test_build_fleet_records_ambiguous_prompt_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(langsmith_fleet.ENV_LANGSMITH_KEY, raising=False)
    context = langsmith_fleet.FleetRunContext(
        run_id="nlq:ambig",
        query_category="ambiguous_query",
    )
    records = langsmith_fleet.build_fleet_records(
        context=context,
        sql_validation_status="ambiguous",
        read_only_status="unknown",
        row_count=0,
        error_code="AMBIGUOUS_PROMPT",
    )
    statuses = {record["operation"]: record["status"] for record in records}
    assert statuses == {
        "sql-generation": "error",
        "validation": "skipped",
        "execution": "skipped",
        "replay": "skipped",
    }
    gen = next(r for r in records if r["operation"] == "sql-generation")
    assert gen["error_category"] == "AMBIGUOUS_PROMPT"
    assert gen["domain"]["sql_validation_status"] == "ambiguous"
    assert gen["domain"]["read_only_status"] == "unknown"


def test_build_fleet_records_invalid_request_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(langsmith_fleet.ENV_LANGSMITH_KEY, raising=False)
    context = langsmith_fleet.FleetRunContext(
        run_id="nlq:invalid",
        query_category="bad_request",
    )
    records = langsmith_fleet.build_fleet_records(
        context=context,
        sql_validation_status="invalid_request",
        read_only_status="unknown",
        row_count=0,
        error_code="INVALID_REQUEST",
    )
    statuses = {record["operation"]: record["status"] for record in records}
    assert statuses["validation"] == "error"
    assert statuses["execution"] == "skipped"
    val = next(r for r in records if r["operation"] == "validation")
    assert val["domain"]["sql_validation_status"] == "invalid_request"
    assert val["domain"]["read_only_status"] == "unknown"


def test_build_fleet_records_with_artifact_ref(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(langsmith_fleet.ENV_LANGSMITH_KEY, raising=False)
    context = langsmith_fleet.FleetRunContext(
        run_id="nlq:ref",
        query_category="test_category",
    )
    records = langsmith_fleet.build_fleet_records(
        context=context,
        sql_validation_status="pass",
        read_only_status="read_only",
        row_count=1,
        artifact_ref="s3://bucket/run-001.json",
    )
    for record in records[:3]:
        assert record["artifact_ref"] == "s3://bucket/run-001.json"


def test_build_fleet_records_from_response_respects_preset_latency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(langsmith_fleet.ENV_LANGSMITH_KEY, raising=False)
    connection = _seed_connection()
    try:
        request = NLToSQLRequest(question="Show funded ratio", max_rows=5)
        response = run_nl_sql_chain(
            connection=connection,
            request=request,
            chain=_StaticChain(
                "SELECT id, value FROM sample_metrics WHERE metric = 'funded_ratio'"
            ),
            trace_sink=None,
            policy=_policy(),
        )
    finally:
        connection.close()

    context = langsmith_fleet.FleetRunContext(
        run_id=response.metadata.request_id,
        query_category="funded_ratio_lookup",
        latency_ms=9999,
    )
    records = langsmith_fleet.build_fleet_records_from_response(
        context=context,
        response=response,
        request=request,
    )
    for record in records[:3]:
        assert record["domain"]["latency_ms"] == 9999


def test_default_fleet_artifact_path_env_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    override = str(tmp_path / "custom" / "fleet.ndjson")
    monkeypatch.setenv("PENSION_DATA_LANGSMITH_FLEET_PATH", override)
    result = langsmith_fleet.default_fleet_artifact_path()
    assert result == (tmp_path / "custom" / "fleet.ndjson")


def test_append_fleet_records_rejects_invalid_retention_limit(tmp_path: Path) -> None:
    path = tmp_path / langsmith_fleet.ARTIFACT_NAME
    with pytest.raises(ValueError, match="retention_limit must be >= 1"):
        langsmith_fleet.append_fleet_records(path, [], retention_limit=0)


def test_append_fleet_records_empty_is_noop(tmp_path: Path) -> None:
    path = tmp_path / langsmith_fleet.ARTIFACT_NAME
    result = langsmith_fleet.append_fleet_records(path, [])
    assert result == path
    assert not path.exists()


def test_build_fleet_records_unknown_error_code_skips_all_stages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An unrecognised error code maps to no known stage (target_position == -1),
    # so every stage appears after the (unknown) failure point and is marked skipped.
    monkeypatch.delenv(langsmith_fleet.ENV_LANGSMITH_KEY, raising=False)
    context = langsmith_fleet.FleetRunContext(
        run_id="nlq:unknown-err",
        query_category="misc",
    )
    records = langsmith_fleet.build_fleet_records(
        context=context,
        sql_validation_status="unknown",
        read_only_status="unknown",
        row_count=0,
        error_code="SOME_UNKNOWN_CODE",
    )
    statuses = {record["operation"]: record["status"] for record in records}
    assert all(s == "skipped" for s in statuses.values())
