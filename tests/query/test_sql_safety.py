"""Regression tests for NL-to-SQL safety validators."""

from __future__ import annotations

import pytest

from pension_data.query.sql_safety import (
    AmbiguousPromptError,
    SQLSafetyPolicy,
    SQLSafetyValidationError,
    _main_select_clause,
    _split_select_expressions,
    _strip_sql_comments_and_strings,
    default_nl_query_policy,
    extract_relations,
    extract_selected_columns,
    validate_nl_prompt,
    validate_read_only_sql,
    validate_result_columns,
    validate_sql_policy,
)

# ── Comment / string stripping ──────────────────────────────────────


class TestStripSqlCommentsAndStrings:
    def test_plain_select_unchanged(self) -> None:
        sql = "SELECT plan_id FROM curated_metric_facts"
        result = _strip_sql_comments_and_strings(sql)
        assert "SELECT" in result
        assert "plan_id" in result
        assert "curated_metric_facts" in result

    def test_single_line_comment_removed(self) -> None:
        sql = "SELECT plan_id -- this is a comment\nFROM curated_metric_facts"
        result = _strip_sql_comments_and_strings(sql)
        assert "this is a comment" not in result
        assert "FROM" in result

    def test_block_comment_removed(self) -> None:
        sql = "SELECT /* hidden */ plan_id FROM curated_metric_facts"
        result = _strip_sql_comments_and_strings(sql)
        assert "hidden" not in result
        assert "plan_id" in result

    def test_single_quoted_string_blanked(self) -> None:
        sql = "SELECT plan_id FROM t WHERE name = 'DROP TABLE'"
        result = _strip_sql_comments_and_strings(sql)
        assert "DROP TABLE" not in result
        assert "WHERE" in result

    def test_double_quoted_string_blanked(self) -> None:
        sql = 'SELECT plan_id FROM t WHERE "colname" = 1'
        result = _strip_sql_comments_and_strings(sql)
        assert "colname" not in result

    def test_escaped_single_quote_inside_string(self) -> None:
        sql = "SELECT plan_id FROM t WHERE val = 'it''s fine'"
        result = _strip_sql_comments_and_strings(sql)
        # The escaped quote content should be blanked
        assert "it" not in result
        assert "fine" not in result

    def test_multiline_block_comment(self) -> None:
        sql = "SELECT plan_id\n/* multi\nline\ncomment */\nFROM t"
        result = _strip_sql_comments_and_strings(sql)
        assert "multi" not in result
        assert "FROM" in result


# ── Read-only SQL validation ────────────────────────────────────────


class TestValidateReadOnlySql:
    def test_simple_select(self) -> None:
        result = validate_read_only_sql("SELECT plan_id FROM curated_metric_facts")
        assert "SELECT" in result

    def test_with_cte(self) -> None:
        sql = "WITH t AS (SELECT plan_id FROM curated_metric_facts) SELECT plan_id FROM t"
        result = validate_read_only_sql(sql)
        assert result.startswith("WITH")

    def test_rejects_insert(self) -> None:
        with pytest.raises(SQLSafetyValidationError, match="read-only"):
            validate_read_only_sql("INSERT INTO curated_metric_facts VALUES (1)")

    def test_rejects_update(self) -> None:
        with pytest.raises(SQLSafetyValidationError, match="read-only"):
            validate_read_only_sql("UPDATE curated_metric_facts SET plan_id = 'x'")

    def test_rejects_delete(self) -> None:
        with pytest.raises(SQLSafetyValidationError, match="read-only"):
            validate_read_only_sql("DELETE FROM curated_metric_facts")

    def test_rejects_drop(self) -> None:
        with pytest.raises(SQLSafetyValidationError, match="read-only"):
            validate_read_only_sql("DROP TABLE curated_metric_facts")

    def test_rejects_multiple_statements(self) -> None:
        with pytest.raises(SQLSafetyValidationError, match="multiple"):
            validate_read_only_sql(
                "SELECT 1; SELECT 2"
            )

    def test_allows_trailing_semicolon(self) -> None:
        result = validate_read_only_sql("SELECT plan_id FROM curated_metric_facts;")
        assert "SELECT" in result
        assert not result.endswith(";")

    def test_rejects_empty(self) -> None:
        with pytest.raises(SQLSafetyValidationError, match="empty"):
            validate_read_only_sql("")

    def test_rejects_pragma(self) -> None:
        with pytest.raises(SQLSafetyValidationError):
            validate_read_only_sql("PRAGMA table_info(curated_metric_facts)")

    def test_forbidden_token_inside_comment_ignored(self) -> None:
        """Tokens inside comments should not trigger the forbidden token check."""
        sql = "SELECT plan_id FROM curated_metric_facts -- DROP TABLE foo"
        result = validate_read_only_sql(sql)
        assert "plan_id" in result

    def test_forbidden_token_inside_string_ignored(self) -> None:
        """Tokens inside string literals should not trigger the forbidden token check."""
        sql = "SELECT plan_id FROM curated_metric_facts WHERE name = 'delete me'"
        result = validate_read_only_sql(sql)
        assert "plan_id" in result


# ── SELECT clause extraction ────────────────────────────────────────


class TestMainSelectClause:
    def test_simple_select(self) -> None:
        sanitized = _strip_sql_comments_and_strings(
            "SELECT plan_id, plan_period FROM curated_metric_facts"
        ).lower()
        clause = _main_select_clause(sanitized)
        assert "plan_id" in clause
        assert "plan_period" in clause

    def test_cte_skips_inner_select(self) -> None:
        sql = "WITH t AS (SELECT inner_col FROM t2) SELECT plan_id FROM t"
        sanitized = _strip_sql_comments_and_strings(sql).lower()
        clause = _main_select_clause(sanitized)
        assert "plan_id" in clause
        assert "inner_col" not in clause


class TestSplitSelectExpressions:
    def test_simple_columns(self) -> None:
        result = _split_select_expressions("plan_id, plan_period, metric_name")
        assert result == ("plan_id", "plan_period", "metric_name")

    def test_nested_parens_not_split(self) -> None:
        result = _split_select_expressions("count(plan_id, extra), metric_name")
        assert len(result) == 2
        assert "count(plan_id, extra)" in result[0]

    def test_single_column(self) -> None:
        result = _split_select_expressions("plan_id")
        assert result == ("plan_id",)


# ── Relation extraction ─────────────────────────────────────────────


class TestExtractRelations:
    def test_simple_from(self) -> None:
        relations = extract_relations("SELECT plan_id FROM curated_metric_facts")
        assert relations == ("curated_metric_facts",)

    def test_join(self) -> None:
        sql = (
            "SELECT a.plan_id FROM curated_metric_facts a "
            "JOIN curated_cash_flow_facts b ON a.plan_id = b.plan_id"
        )
        relations = extract_relations(sql)
        assert "curated_metric_facts" in relations
        assert "curated_cash_flow_facts" in relations

    def test_cte_alias_excluded(self) -> None:
        sql = (
            "WITH recent AS (SELECT plan_id FROM curated_metric_facts) "
            "SELECT plan_id FROM recent"
        )
        relations = extract_relations(sql)
        assert "recent" not in relations
        assert "curated_metric_facts" in relations

    def test_schema_qualified_table(self) -> None:
        sql = "SELECT plan_id FROM public.curated_metric_facts"
        relations = extract_relations(sql)
        assert "curated_metric_facts" in relations


# ── Extract selected columns ────────────────────────────────────────


class TestExtractSelectedColumns:
    def test_simple_projection(self) -> None:
        cols = extract_selected_columns("SELECT plan_id, metric_name FROM curated_metric_facts")
        assert cols == ("plan_id", "metric_name")

    def test_distinct_stripped(self) -> None:
        cols = extract_selected_columns(
            "SELECT DISTINCT plan_id FROM curated_metric_facts"
        )
        assert cols == ("plan_id",)

    def test_star_rejected(self) -> None:
        with pytest.raises(SQLSafetyValidationError, match="SELECT \\*"):
            extract_selected_columns("SELECT * FROM curated_metric_facts")

    def test_table_qualified_column(self) -> None:
        cols = extract_selected_columns(
            "SELECT c.plan_id FROM curated_metric_facts c"
        )
        assert cols == ("plan_id",)


# ── Policy validation ───────────────────────────────────────────────


class TestValidateSqlPolicy:
    @pytest.fixture()
    def strict_policy(self) -> SQLSafetyPolicy:
        return default_nl_query_policy()

    def test_valid_query_passes(self, strict_policy: SQLSafetyPolicy) -> None:
        sql = "SELECT plan_id, metric_name FROM curated_metric_facts"
        result = validate_sql_policy(sql, policy=strict_policy)
        assert "plan_id" in result

    def test_disallowed_relation_rejected(self, strict_policy: SQLSafetyPolicy) -> None:
        with pytest.raises(SQLSafetyValidationError, match="disallowed relation"):
            validate_sql_policy(
                "SELECT plan_id FROM secret_table",
                policy=strict_policy,
            )

    def test_disallowed_column_rejected(self, strict_policy: SQLSafetyPolicy) -> None:
        with pytest.raises(SQLSafetyValidationError, match="disallowed column"):
            validate_sql_policy(
                "SELECT secret_col FROM curated_metric_facts",
                policy=strict_policy,
            )

    def test_limit_exceeding_max_rows_rejected(self, strict_policy: SQLSafetyPolicy) -> None:
        with pytest.raises(SQLSafetyValidationError, match="LIMIT"):
            validate_sql_policy(
                "SELECT plan_id FROM curated_metric_facts LIMIT 9999",
                policy=strict_policy,
            )

    def test_limit_within_max_rows_passes(self, strict_policy: SQLSafetyPolicy) -> None:
        result = validate_sql_policy(
            "SELECT plan_id FROM curated_metric_facts LIMIT 100",
            policy=strict_policy,
        )
        assert "LIMIT" in result

    def test_banned_clause_rejected(self, strict_policy: SQLSafetyPolicy) -> None:
        with pytest.raises(SQLSafetyValidationError, match="banned clause"):
            validate_sql_policy(
                "SELECT plan_id FROM curated_metric_facts INTO OUTFILE x",
                policy=strict_policy,
            )

    def test_comma_join_rejected(self, strict_policy: SQLSafetyPolicy) -> None:
        with pytest.raises(SQLSafetyValidationError, match="comma joins"):
            validate_sql_policy(
                "SELECT plan_id FROM curated_metric_facts, curated_cash_flow_facts",
                policy=strict_policy,
            )

    def test_quoted_identifier_rejected(self, strict_policy: SQLSafetyPolicy) -> None:
        with pytest.raises(SQLSafetyValidationError, match="quoted identifiers"):
            validate_sql_policy(
                'SELECT "plan_id" FROM curated_metric_facts',
                policy=strict_policy,
            )

    def test_select_star_rejected(self, strict_policy: SQLSafetyPolicy) -> None:
        with pytest.raises(SQLSafetyValidationError, match="SELECT \\*"):
            validate_sql_policy(
                "SELECT * FROM curated_metric_facts",
                policy=strict_policy,
            )

    def test_no_relation_rejected(self, strict_policy: SQLSafetyPolicy) -> None:
        with pytest.raises(SQLSafetyValidationError, match="at least one allowed relation"):
            validate_sql_policy(
                "SELECT 1",
                policy=strict_policy,
            )


# ── Result column validation ────────────────────────────────────────


class TestValidateResultColumns:
    def test_allowed_columns_pass(self) -> None:
        policy = default_nl_query_policy()
        validate_result_columns(("plan_id", "metric_name"), policy=policy)

    def test_disallowed_column_rejected(self) -> None:
        policy = default_nl_query_policy()
        with pytest.raises(SQLSafetyValidationError, match="disallowed column"):
            validate_result_columns(("plan_id", "secret_col"), policy=policy)

    def test_empty_allowed_columns_skips(self) -> None:
        policy = SQLSafetyPolicy(
            allowed_relations=(),
            allowed_columns=(),
            banned_clauses=(),
            max_rows=100,
            max_timeout_ms=1000,
        )
        # Should not raise — no allowlist means no enforcement
        validate_result_columns(("anything",), policy=policy)


# ── NL prompt validation ────────────────────────────────────────────


class TestValidateNlPrompt:
    def test_valid_question(self) -> None:
        result = validate_nl_prompt("What is the funded ratio for CalPERS?")
        assert "funded ratio" in result

    def test_empty_rejected(self) -> None:
        with pytest.raises(AmbiguousPromptError, match="required"):
            validate_nl_prompt("")

    def test_whitespace_only_rejected(self) -> None:
        with pytest.raises(AmbiguousPromptError, match="required"):
            validate_nl_prompt("   ")

    def test_too_few_tokens_rejected(self) -> None:
        with pytest.raises(AmbiguousPromptError, match="ambiguous"):
            validate_nl_prompt("show me")

    def test_whitespace_normalized(self) -> None:
        result = validate_nl_prompt("  what   is   the   funded  ratio ?")
        assert "  " not in result
