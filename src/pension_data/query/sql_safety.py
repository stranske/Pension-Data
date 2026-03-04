"""Read-only SQL and NL prompt safety validators for NL-to-SQL workflows."""

from __future__ import annotations

import re
from dataclasses import dataclass

_FORBIDDEN_SQL_TOKENS: tuple[str, ...] = (
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "replace",
    "attach",
    "detach",
    "pragma",
    "vacuum",
    "reindex",
    "truncate",
)
_SQL_WORD_PATTERN = re.compile(r"\b[a-z_][a-z0-9_]*\b", re.IGNORECASE)
_TOKEN_PATTERN = re.compile(r"[a-zA-Z0-9_]+")
_RELATION_PATTERN = re.compile(r"\b(?:from|join)\s+([a-z_][a-z0-9_\.]*)", re.IGNORECASE)
_CTE_ALIAS_PATTERN = re.compile(
    r"\b([a-z_][a-z0-9_]*)\s*(?:\([^)]*\))?\s+as\s*\(",
    re.IGNORECASE,
)
_LIMIT_TOKEN_PATTERN = re.compile(r"\blimit\s+([^\s;]+)", re.IGNORECASE)
_COMMA_JOIN_PATTERN = re.compile(r"\bfrom\b[^;]*,", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class SQLSafetyPolicy:
    """Policy envelope for strict NL SQL execution."""

    allowed_relations: tuple[str, ...]
    allowed_columns: tuple[str, ...]
    banned_clauses: tuple[str, ...]
    max_rows: int
    max_timeout_ms: int


def default_nl_query_policy() -> SQLSafetyPolicy:
    """Return strict default policy for Pension-Data NL query execution."""
    return SQLSafetyPolicy(
        allowed_relations=(
            "curated_metric_facts",
            "curated_cash_flow_facts",
        ),
        allowed_columns=(
            "plan_id",
            "plan_period",
            "metric_family",
            "metric_name",
            "normalized_value",
            "normalized_unit",
            "manager_name",
            "fund_name",
            "vehicle_name",
            "beginning_aum_normalized",
            "ending_aum_normalized",
            "employer_contributions_normalized",
            "employee_contributions_normalized",
            "benefit_payments_normalized",
            "refunds_normalized",
            "effective_date",
            "ingestion_date",
            "benchmark_version",
            "source_document_id",
        ),
        banned_clauses=(
            "pragma",
            "into outfile",
            "copy ",
            "pg_catalog",
            "information_schema",
        ),
        max_rows=500,
        max_timeout_ms=2_000,
    )


class SQLSafetyValidationError(ValueError):
    """Raised when generated SQL violates read-only safety policy."""


class AmbiguousPromptError(ValueError):
    """Raised when NL prompt content is too ambiguous for deterministic SQL generation."""


def validate_nl_prompt(question: str) -> str:
    """Validate NL prompt content and reject low-information or ambiguous prompts."""
    normalized = " ".join(question.strip().split())
    if not normalized:
        raise AmbiguousPromptError("question is required")

    alnum_tokens = [token for token in _TOKEN_PATTERN.findall(normalized) if token]
    if len(alnum_tokens) < 3:
        raise AmbiguousPromptError("question is ambiguous; provide a more specific request")
    return normalized


def _strip_sql_comments_and_strings(sql: str) -> str:
    result: list[str] = []
    index = 0
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False

    while index < len(sql):
        current = sql[index]
        nxt = sql[index + 1] if index + 1 < len(sql) else ""

        if in_line_comment:
            if current == "\n":
                in_line_comment = False
                result.append("\n")
            else:
                result.append(" ")
            index += 1
            continue
        if in_block_comment:
            if current == "*" and nxt == "/":
                in_block_comment = False
                result.extend((" ", " "))
                index += 2
            else:
                result.append(" ")
                index += 1
            continue
        if in_single:
            if current == "'" and nxt == "'":
                result.extend((" ", " "))
                index += 2
                continue
            if current == "'":
                in_single = False
            result.append(" ")
            index += 1
            continue
        if in_double:
            if current == '"' and nxt == '"':
                result.extend((" ", " "))
                index += 2
                continue
            if current == '"':
                in_double = False
            result.append(" ")
            index += 1
            continue

        if current == "-" and nxt == "-":
            in_line_comment = True
            result.extend((" ", " "))
            index += 2
            continue
        if current == "/" and nxt == "*":
            in_block_comment = True
            result.extend((" ", " "))
            index += 2
            continue
        if current == "'":
            in_single = True
            result.append(" ")
            index += 1
            continue
        if current == '"':
            in_double = True
            result.append(" ")
            index += 1
            continue

        result.append(current)
        index += 1
    return "".join(result)


def validate_read_only_sql(sql: str) -> str:
    """Validate generated SQL and return normalized read-only statement text."""
    normalized = sql.strip()
    if not normalized:
        raise SQLSafetyValidationError("generated SQL is empty")

    sanitized = _strip_sql_comments_and_strings(normalized)
    semicolon_indices = [index for index, char in enumerate(sanitized) if char == ";"]
    if len(semicolon_indices) > 1:
        raise SQLSafetyValidationError("multiple SQL statements are not allowed")
    if len(semicolon_indices) == 1:
        semicolon_index = semicolon_indices[0]
        if any(not char.isspace() for char in sanitized[semicolon_index + 1 :]):
            raise SQLSafetyValidationError("multiple SQL statements are not allowed")
        normalized = normalized[:semicolon_index].strip()
        sanitized = sanitized[:semicolon_index].strip()

    if not normalized:
        raise SQLSafetyValidationError("generated SQL is empty")

    lowered = sanitized.lower().lstrip()
    if not lowered.startswith(("select", "with")):
        raise SQLSafetyValidationError("only read-only SELECT/WITH queries are allowed")

    tokens = {match.group(0).lower() for match in _SQL_WORD_PATTERN.finditer(sanitized)}
    forbidden = sorted(token for token in _FORBIDDEN_SQL_TOKENS if token in tokens)
    if forbidden:
        raise SQLSafetyValidationError(
            "generated SQL contains forbidden token(s): " + ", ".join(forbidden)
        )

    return normalized


def _sanitize_sql(sql: str) -> str:
    return _strip_sql_comments_and_strings(sql).lower()


def _main_select_clause(sanitized_sql: str) -> str:
    depth = 0
    select_start: int | None = None
    token_start: int | None = None
    index = 0
    while index < len(sanitized_sql):
        char = sanitized_sql[index]
        if char == "(":
            depth += 1
            index += 1
            continue
        if char == ")":
            depth = max(0, depth - 1)
            index += 1
            continue
        if char.isalnum() or char == "_":
            if token_start is None:
                token_start = index
            index += 1
            continue
        if token_start is not None:
            token = sanitized_sql[token_start:index]
            if depth == 0 and token == "select":
                select_start = index
            elif depth == 0 and token == "from" and select_start is not None:
                return sanitized_sql[select_start:token_start].strip()
            token_start = None
        index += 1
    if token_start is not None:
        token = sanitized_sql[token_start:index]
        if depth == 0 and token == "from" and select_start is not None:
            return sanitized_sql[select_start:token_start].strip()
    raise SQLSafetyValidationError("generated SQL must include SELECT ... FROM structure")


def _split_select_expressions(select_clause: str) -> tuple[str, ...]:
    expressions: list[str] = []
    current: list[str] = []
    depth = 0
    for char in select_clause:
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        if char == "," and depth == 0:
            token = "".join(current).strip()
            if token:
                expressions.append(token)
            current = []
            continue
        current.append(char)
    token = "".join(current).strip()
    if token:
        expressions.append(token)
    return tuple(expressions)


def _validate_select_column_allowlist(
    *,
    sanitized_sql: str,
    allowed_columns: tuple[str, ...],
) -> None:
    if not allowed_columns:
        return
    if _COMMA_JOIN_PATTERN.search(sanitized_sql):
        raise SQLSafetyValidationError("comma joins are not allowed")
    select_clause = _main_select_clause(sanitized_sql)
    expressions = _split_select_expressions(select_clause)
    if not expressions:
        raise SQLSafetyValidationError("generated SQL must project at least one column")
    allowed = {column.lower() for column in allowed_columns}
    for index, expression in enumerate(expressions):
        normalized = expression.strip()
        if index == 0 and normalized.startswith("distinct "):
            normalized = normalized[len("distinct ") :].strip()
        if normalized == "*":
            raise SQLSafetyValidationError("SELECT * is not allowed")
        match = re.fullmatch(r"(?:[a-z_][a-z0-9_]*\.)?([a-z_][a-z0-9_]*)", normalized)
        if match is None:
            raise SQLSafetyValidationError(
                "SELECT expressions must be direct column references without aliases"
            )
        column_name = match.group(1).lower()
        if column_name not in allowed:
            raise SQLSafetyValidationError(
                f"generated SQL references disallowed column '{column_name}'"
            )


def _extract_cte_aliases(sanitized_sql: str) -> set[str]:
    return {match.group(1).lower() for match in _CTE_ALIAS_PATTERN.finditer(sanitized_sql)}


def extract_relations(sql: str) -> tuple[str, ...]:
    """Extract relation identifiers referenced in FROM/JOIN clauses."""
    sanitized_sql = _sanitize_sql(sql)
    cte_aliases = _extract_cte_aliases(sanitized_sql)
    relations: set[str] = set()
    for match in _RELATION_PATTERN.finditer(sanitized_sql):
        token = match.group(1).split(".")[-1].strip().lower()
        if not token or token in cte_aliases:
            continue
        relations.add(token)
    return tuple(sorted(relations))


def validate_sql_policy(sql: str, *, policy: SQLSafetyPolicy) -> str:
    """Validate SQL text under statement/relation/banned-clause policy."""
    normalized = validate_read_only_sql(sql)
    if '"' in normalized:
        raise SQLSafetyValidationError("quoted identifiers are not allowed")
    sanitized_sql = _sanitize_sql(normalized)
    if _COMMA_JOIN_PATTERN.search(sanitized_sql):
        raise SQLSafetyValidationError("comma joins are not allowed")

    banned_hits = sorted(clause for clause in policy.banned_clauses if clause in sanitized_sql)
    if banned_hits:
        raise SQLSafetyValidationError(
            "generated SQL contains banned clause(s): " + ", ".join(banned_hits)
        )

    for limit_match in _LIMIT_TOKEN_PATTERN.finditer(sanitized_sql):
        limit_token = limit_match.group(1).strip()
        if not re.fullmatch(r"\d+", limit_token):
            raise SQLSafetyValidationError("generated SQL LIMIT must be a positive integer literal")
        limit_value = int(limit_token)
        if limit_value > policy.max_rows:
            raise SQLSafetyValidationError(
                f"generated SQL LIMIT ({limit_value}) exceeds policy max_rows ({policy.max_rows})"
            )

    referenced_relations = extract_relations(normalized)
    if policy.allowed_relations and not referenced_relations:
        raise SQLSafetyValidationError("generated SQL must reference at least one allowed relation")
    if policy.allowed_relations:
        allowed = {value.lower() for value in policy.allowed_relations}
        disallowed = sorted(
            relation for relation in referenced_relations if relation not in allowed
        )
        if disallowed:
            raise SQLSafetyValidationError(
                "generated SQL references disallowed relation(s): " + ", ".join(disallowed)
            )
    _validate_select_column_allowlist(
        sanitized_sql=sanitized_sql,
        allowed_columns=policy.allowed_columns,
    )
    return normalized


def validate_result_columns(columns: tuple[str, ...], *, policy: SQLSafetyPolicy) -> None:
    """Validate returned column names against allowlist policy."""
    if not policy.allowed_columns:
        return
    allowed = {column.lower() for column in policy.allowed_columns}
    disallowed = sorted(
        {column for column in columns if column.strip() and column.lower() not in allowed}
    )
    if disallowed:
        raise SQLSafetyValidationError(
            "generated SQL returns disallowed column(s): " + ", ".join(disallowed)
        )
