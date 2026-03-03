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
        "CREATE TABLE sample_metrics (id INTEGER PRIMARY KEY, metric TEXT NOT NULL, value REAL NOT NULL)"
    )
    connection.executemany(
        "INSERT INTO sample_metrics (id, metric, value) VALUES (?, ?, ?)",
        [
            (1, "funded_ratio", 0.79),
            (2, "funded_ratio", 0.81),
            (3, "discount_rate", 0.0675),
        ],
    )
    return connection


def test_sql_safety_validator_allows_read_only_and_rejects_destructive_queries() -> None:
    assert (
        validate_read_only_sql("SELECT id, value FROM sample_metrics ORDER BY id;")
        == "SELECT id, value FROM sample_metrics ORDER BY id"
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
        )
    finally:
        connection.close()

    assert response.status == "ok"
    assert response.error is None
    assert response.sql is not None
    assert response.columns == ("id", "value")
    assert response.rows == ((1, 0.79), (2, 0.81))
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
