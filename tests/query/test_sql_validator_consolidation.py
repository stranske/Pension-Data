"""#638: one read-only SQL validator, shared by both consumers.

`query/sql_service.execute_sql_query` and `langchain/eval_harness` (via
`validate_read_only_sql`) must reach the SAME accept/reject decision. Documented
semicolon policy: a single trailing `;` is allowed (stripped); any statement after
a `;` is rejected as multi-statement.
"""

from __future__ import annotations

import sqlite3

import pytest

from pension_data.query.sql_safety import SQLSafetyValidationError, validate_read_only_sql
from pension_data.query.sql_service import SQLQueryRequest, execute_sql_query


def _seed() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id INTEGER)")
    conn.execute("INSERT INTO t (id) VALUES (1)")
    conn.commit()
    return conn


def _service_ok(sql: str) -> bool:
    resp = execute_sql_query(
        connection=_seed(),
        request=SQLQueryRequest(sql=sql),
        caller_key_id="key:test",
    )
    return resp.status == "ok"


def _safety_ok(sql: str) -> bool:
    try:
        validate_read_only_sql(sql)
        return True
    except SQLSafetyValidationError:
        return False


@pytest.mark.parametrize(
    ("sql", "accepted"),
    [
        ("SELECT id FROM t", True),
        ("SELECT id FROM t;", True),  # single trailing semicolon allowed
        ("SELECT id FROM t ;  ", True),  # trailing ; + whitespace allowed
        ("SELECT id FROM t; SELECT id FROM t", False),  # multi-statement rejected
        ("SELECT id FROM t; DROP TABLE t", False),  # injected 2nd statement rejected
        ("DELETE FROM t", False),  # non-read-only rejected
    ],
)
def test_both_paths_agree(sql: str, accepted: bool) -> None:
    assert _safety_ok(sql) is accepted
    assert _service_ok(sql) is accepted


def test_trailing_semicolon_is_stripped_by_the_single_validator() -> None:
    assert validate_read_only_sql("SELECT id FROM t;") == "SELECT id FROM t"
