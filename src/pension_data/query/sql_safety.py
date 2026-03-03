"""Read-only SQL and NL prompt safety validators for NL-to-SQL workflows."""

from __future__ import annotations

import re

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
