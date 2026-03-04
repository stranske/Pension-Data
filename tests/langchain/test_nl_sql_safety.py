"""Tests for NL-to-SQL safety enforcement and trace emission."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from typing import Any

import pytest

from pension_data.api.auth import SCOPE_NL, SCOPE_QUERY, APIKeyStore, ScopeDeniedError
from pension_data.api.routes.nl import run_nl_query_endpoint
from pension_data.langchain.nl_sql_chain import (
    InMemoryLangSmithTraceSink,
    NLToSQLRequest,
    run_nl_sql_chain,
)
from pension_data.query.sql_safety import (
    AmbiguousPromptError,
    SQLSafetyPolicy,
    SQLSafetyValidationError,
    validate_nl_prompt,
    validate_read_only_sql,
)


class StaticChain:
    """Deterministic test chain returning a configured SQL payload."""

    def __init__(self, output: str | Mapping[str, Any]) -> None:
        self.output = output

    def invoke(self, values: Mapping[str, Any]) -> str | Mapping[str, Any]:
        del values
        return self.output


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
        "INSERT INTO sample_metrics (id, metric, value, source_document_id, evidence_refs, confidence) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, "funded_ratio", 0.79, "doc:1", '["p.10"]', 0.9),
            (2, "funded_ratio", 0.81, "doc:1", '["p.11"]', 0.85),
            (3, "discount_rate", 0.0675, "doc:2", '["p.20"]', 0.8),
        ],
    )
    return connection


def _sample_policy(
    *,
    allowed_relations: tuple[str, ...] = ("sample_metrics",),
    allowed_columns: tuple[str, ...] = (
        "id",
        "metric",
        "value",
        "source_document_id",
        "evidence_refs",
        "confidence",
    ),
    max_rows: int = 500,
    max_timeout_ms: int = 2_000,
    require_source_document_id: bool = False,
) -> SQLSafetyPolicy:
    return SQLSafetyPolicy(
        allowed_relations=allowed_relations,
        allowed_columns=allowed_columns,
        banned_clauses=("pragma", "into outfile", "copy ", "pg_catalog", "information_schema"),
        max_rows=max_rows,
        max_timeout_ms=max_timeout_ms,
        require_source_document_id=require_source_document_id,
    )


def test_sql_safety_validator_allows_read_only_and_rejects_destructive_queries() -> None:
    assert (
        validate_read_only_sql("SELECT id, value FROM sample_metrics ORDER BY id;")
        == "SELECT id, value FROM sample_metrics ORDER BY id"
    )
    assert (
        validate_read_only_sql("SELECT id FROM sample_metrics; -- trailing comment")
        == "SELECT id FROM sample_metrics"
    )
    with pytest.raises(SQLSafetyValidationError, match="SELECT/WITH"):
        validate_read_only_sql("DELETE FROM sample_metrics")
    with pytest.raises(SQLSafetyValidationError, match="multiple SQL statements"):
        validate_read_only_sql("SELECT id FROM sample_metrics; SELECT value FROM sample_metrics")


def test_nl_prompt_validator_rejects_ambiguous_questions() -> None:
    with pytest.raises(AmbiguousPromptError, match="question is required"):
        validate_nl_prompt("   ")
    with pytest.raises(AmbiguousPromptError, match="question is ambiguous"):
        validate_nl_prompt("ratio?")


def test_nl_sql_chain_executes_read_only_sql_and_emits_langsmith_traces() -> None:
    connection = _seed_connection()
    traces = InMemoryLangSmithTraceSink(events=[])
    try:
        response = run_nl_sql_chain(
            connection=connection,
            request=NLToSQLRequest(
                question="Show funded ratio values by id",
                max_rows=10,
            ),
            chain=StaticChain("SELECT id, value FROM sample_metrics WHERE metric = 'funded_ratio'"),
            trace_sink=traces,
            policy=_sample_policy(),
        )
    finally:
        connection.close()

    assert response.status == "ok"
    assert response.error is None
    assert response.sql is not None
    assert response.columns == ("id", "value")
    assert response.rows == ((1, 0.79), (2, 0.81))
    assert len(response.provenance) == 2
    assert response.provenance[0].row_index == 0
    assert response.provenance[0].source_document_id is None
    assert response.provenance[0].evidence_refs == ()
    assert response.provenance[0].confidence is None
    assert [event.stage for event in traces.events] == [
        "nl.prompt.received",
        "nl.sql.generated",
        "nl.sql.executed",
    ]


def test_nl_sql_chain_rejects_unsafe_generated_sql_and_emits_error_trace() -> None:
    connection = _seed_connection()
    traces = InMemoryLangSmithTraceSink(events=[])
    try:
        response = run_nl_sql_chain(
            connection=connection,
            request=NLToSQLRequest(question="Delete all funded rows now"),
            chain=StaticChain("DELETE FROM sample_metrics"),
            trace_sink=traces,
            policy=_sample_policy(),
        )
        remaining_rows = connection.execute("SELECT COUNT(*) FROM sample_metrics").fetchone()[0]
    finally:
        connection.close()

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "UNSAFE_SQL"
    assert remaining_rows == 3
    assert traces.events[-1].stage == "nl.sql.error"
    assert traces.events[-1].payload["error_code"] == "UNSAFE_SQL"


def test_nl_sql_chain_returns_specific_error_for_max_rows_overflow() -> None:
    connection = _seed_connection()
    traces = InMemoryLangSmithTraceSink(events=[])
    try:
        response = run_nl_sql_chain(
            connection=connection,
            request=NLToSQLRequest(
                question="Show all metric values by id",
                max_rows=2,
            ),
            chain=StaticChain("SELECT id, value FROM sample_metrics ORDER BY id"),
            trace_sink=traces,
            policy=_sample_policy(max_rows=2),
        )
    finally:
        connection.close()

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "MAX_ROWS_EXCEEDED"
    assert "max_rows limit" in response.error.message
    assert traces.events[-1].stage == "nl.sql.error"
    assert traces.events[-1].payload["error_code"] == "MAX_ROWS_EXCEEDED"


def test_nl_sql_chain_timeout_path_emits_timeout_error_code() -> None:
    connection = _seed_connection()
    traces = InMemoryLangSmithTraceSink(events=[])
    recursive_sql = """
        WITH RECURSIVE seq(x) AS (
            SELECT 1
            UNION ALL
            SELECT x + 1 FROM seq WHERE x < 50000000
        )
        SELECT sum(x) FROM seq
    """
    try:
        response = run_nl_sql_chain(
            connection=connection,
            request=NLToSQLRequest(
                question="List recursive sequence rows for diagnostics",
                timeout_ms=1,
                max_rows=100,
            ),
            chain=StaticChain(recursive_sql),
            trace_sink=traces,
            policy=_sample_policy(allowed_relations=(), allowed_columns=()),
        )
    finally:
        connection.close()

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "TIMEOUT"
    assert traces.events[-1].stage == "nl.sql.error"
    assert traces.events[-1].payload["error_code"] == "TIMEOUT"


def test_nl_route_requires_nl_scope_and_emits_audit_event() -> None:
    key_store = APIKeyStore()
    unauthorized_secret, _ = key_store.create_key(scopes=(SCOPE_QUERY,))
    connection = _seed_connection()
    try:
        with pytest.raises(ScopeDeniedError):
            run_nl_query_endpoint(
                api_key_header=unauthorized_secret,
                key_store=key_store,
                connection=connection,
                request=NLToSQLRequest(question="Show funded ratio values by id"),
                chain=StaticChain("SELECT id, value FROM sample_metrics"),
                policy=_sample_policy(),
            )
    finally:
        connection.close()

    authorized_secret, record = key_store.create_key(scopes=(SCOPE_NL,), label="nl-client")
    traces = InMemoryLangSmithTraceSink(events=[])
    connection = _seed_connection()
    try:
        result = run_nl_query_endpoint(
            api_key_header=authorized_secret,
            key_store=key_store,
            connection=connection,
            request=NLToSQLRequest(question="Show funded ratio values by id"),
            chain=StaticChain("SELECT id, value FROM sample_metrics WHERE metric = 'funded_ratio'"),
            trace_sink=traces,
            policy=_sample_policy(),
            event={"request_origin": "unit-test"},
        )
    finally:
        connection.close()

    assert result.response.status == "ok"
    assert result.audit_event["operation"] == "nl.ask"
    assert result.audit_event["api_key_id"] == record.key_id
    assert result.audit_event["request_origin"] == "unit-test"
    assert result.audit_event["query_status"] == "ok"
    assert result.audit_event["returned_rows"] == 2
    assert len(traces.events) == 3


def test_nl_sql_chain_rejects_disallowed_relation() -> None:
    connection = _seed_connection()
    try:
        response = run_nl_sql_chain(
            connection=connection,
            request=NLToSQLRequest(question="List all values by id"),
            chain=StaticChain("SELECT id, value FROM sample_metrics"),
            policy=_sample_policy(allowed_relations=("curated_metric_facts",)),
        )
    finally:
        connection.close()

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "UNSAFE_SQL"
    assert "disallowed relation" in response.error.message


def test_nl_sql_chain_rejects_disallowed_result_columns() -> None:
    connection = _seed_connection()
    try:
        response = run_nl_sql_chain(
            connection=connection,
            request=NLToSQLRequest(question="List metric labels"),
            chain=StaticChain("SELECT id, metric FROM sample_metrics"),
            policy=_sample_policy(allowed_columns=("id",)),
        )
    finally:
        connection.close()

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "UNSAFE_SQL"
    assert "disallowed column" in response.error.message


def test_nl_sql_chain_rejects_sql_limit_above_policy_max_rows() -> None:
    connection = _seed_connection()
    try:
        response = run_nl_sql_chain(
            connection=connection,
            request=NLToSQLRequest(question="List all metric values", max_rows=100),
            chain=StaticChain("SELECT id, value FROM sample_metrics LIMIT 1000"),
            policy=_sample_policy(max_rows=100),
        )
    finally:
        connection.close()

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "UNSAFE_SQL"
    assert "exceeds policy max_rows" in response.error.message


def test_nl_sql_chain_rejects_non_literal_sql_limit() -> None:
    connection = _seed_connection()
    try:
        response = run_nl_sql_chain(
            connection=connection,
            request=NLToSQLRequest(question="List all metric values", max_rows=100),
            chain=StaticChain("SELECT id, value FROM sample_metrics LIMIT ?"),
            policy=_sample_policy(max_rows=100),
        )
    finally:
        connection.close()

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "UNSAFE_SQL"
    assert "LIMIT must be a positive integer literal" in response.error.message


def test_nl_sql_chain_rejects_quoted_identifiers() -> None:
    connection = _seed_connection()
    try:
        response = run_nl_sql_chain(
            connection=connection,
            request=NLToSQLRequest(question="Show all metric values"),
            chain=StaticChain('SELECT "id" FROM sample_metrics'),
            policy=_sample_policy(),
        )
    finally:
        connection.close()

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "UNSAFE_SQL"
    assert "quoted identifiers are not allowed" in response.error.message


def test_nl_sql_chain_rejects_comma_joins() -> None:
    connection = _seed_connection()
    try:
        response = run_nl_sql_chain(
            connection=connection,
            request=NLToSQLRequest(question="Compare values across two aliases"),
            chain=StaticChain(
                "SELECT a.id, b.value FROM sample_metrics a, sample_metrics b WHERE a.id = b.id"
            ),
            policy=_sample_policy(),
        )
    finally:
        connection.close()

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "UNSAFE_SQL"
    assert "comma joins are not allowed" in response.error.message


def test_nl_sql_chain_rejects_select_alias_bypass_attempt() -> None:
    connection = _seed_connection()
    try:
        response = run_nl_sql_chain(
            connection=connection,
            request=NLToSQLRequest(question="Show plan ids"),
            chain=StaticChain("SELECT evidence_refs AS plan_id FROM sample_metrics"),
            policy=_sample_policy(allowed_columns=("plan_id",)),
        )
    finally:
        connection.close()

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "UNSAFE_SQL"
    assert "direct column references" in response.error.message


def test_nl_sql_chain_returns_invalid_request_for_policy_bound_violation() -> None:
    connection = _seed_connection()
    try:
        response = run_nl_sql_chain(
            connection=connection,
            request=NLToSQLRequest(question="Show funded ratio values by id", max_rows=5000),
            chain=StaticChain("SELECT id, value FROM sample_metrics"),
            policy=_sample_policy(max_rows=100),
        )
    finally:
        connection.close()

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "INVALID_REQUEST"
    assert "policy max_rows" in response.error.message


def test_nl_sql_chain_emits_provenance_metadata_when_fields_present() -> None:
    connection = _seed_connection()
    try:
        response = run_nl_sql_chain(
            connection=connection,
            request=NLToSQLRequest(question="Show sources for funded ratio"),
            chain=StaticChain(
                "SELECT id, source_document_id, evidence_refs, confidence "
                "FROM sample_metrics WHERE metric = 'funded_ratio' ORDER BY id"
            ),
            policy=_sample_policy(),
        )
    finally:
        connection.close()

    assert response.status == "ok"
    assert response.error is None
    assert len(response.provenance) == 2
    assert response.provenance[0].source_document_id == "doc:1"
    assert response.provenance[0].evidence_refs == ("p.10",)
    assert response.provenance[0].confidence == pytest.approx(0.9)


def test_nl_sql_chain_requires_source_document_id_when_policy_enforces_provenance() -> None:
    connection = _seed_connection()
    try:
        response = run_nl_sql_chain(
            connection=connection,
            request=NLToSQLRequest(question="Show funded ratio values"),
            chain=StaticChain("SELECT id, value FROM sample_metrics WHERE metric = 'funded_ratio'"),
            policy=_sample_policy(require_source_document_id=True),
        )
    finally:
        connection.close()

    assert response.status == "error"
    assert response.error is not None
    assert response.error.code == "UNSAFE_SQL"
    assert "must include source_document_id" in response.error.message


def test_nl_sql_chain_accepts_source_document_id_when_policy_enforces_provenance() -> None:
    connection = _seed_connection()
    try:
        response = run_nl_sql_chain(
            connection=connection,
            request=NLToSQLRequest(question="Show funded ratio sources"),
            chain=StaticChain(
                "SELECT id, source_document_id FROM sample_metrics WHERE metric = 'funded_ratio'"
            ),
            policy=_sample_policy(require_source_document_id=True),
        )
    finally:
        connection.close()

    assert response.status == "ok"
    assert response.error is None
    assert response.columns == ("id", "source_document_id")
    assert all(row.source_document_id is not None for row in response.provenance)
