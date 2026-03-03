"""Prompt and disclaimer helpers shared across NL query and findings chains."""

from __future__ import annotations

from collections.abc import Sequence

DEFAULT_ANALYTICS_DISCLAIMER = (
    "This is analytical output, not financial advice. Always verify metrics independently."
)

_DEFAULT_NL_QUERY_RULES: tuple[str, ...] = (
    "Generate read-only SQL only; never produce UPDATE/INSERT/DELETE/DDL statements.",
    "Use clear aliases and deterministic ordering when possible.",
    "If the question is ambiguous, ask for clarification instead of guessing hidden intent.",
)


def append_analytics_disclaimer(
    text: str, *, disclaimer: str = DEFAULT_ANALYTICS_DISCLAIMER
) -> str:
    """Append the required disclaimer exactly once to generated output text."""
    body = text.strip()
    disclaimer_text = disclaimer.strip()
    if not body:
        return disclaimer_text
    if body.endswith(disclaimer_text):
        return body
    return f"{body}\n\n{disclaimer_text}"


def build_nl_query_system_prompt(
    *,
    schema_context: str,
    additional_context: str | None = None,
    safety_rules: Sequence[str] | None = None,
) -> str:
    """Build a deterministic system prompt for NL-to-SQL generation."""
    schema_text = schema_context.strip()
    if not schema_text:
        raise ValueError("schema_context must be a non-empty string")
    rules = tuple(safety_rules) if safety_rules is not None else _DEFAULT_NL_QUERY_RULES
    if not rules:
        raise ValueError("safety_rules must include at least one rule")
    rules_text = "\n".join(f"- {rule.strip()}" for rule in rules if rule.strip())
    if not rules_text:
        raise ValueError("safety_rules must include at least one non-empty rule")

    sections = [
        "You are an analyst assistant for Pension-Data.",
        "Convert user questions into safe, read-only SQL over the provided schema context.",
        "",
        "SCHEMA CONTEXT:",
        schema_text,
        "",
        "SAFETY RULES:",
        rules_text,
    ]
    context = (additional_context or "").strip()
    if context:
        sections.extend(["", "ADDITIONAL CONTEXT:", context])
    return "\n".join(sections).strip()


def build_findings_explainer_prompt(
    *,
    findings_context: str,
    user_question: str,
    additional_context: str | None = None,
) -> str:
    """Build a deterministic findings-explainer prompt with disclaimer requirement."""
    findings_text = findings_context.strip()
    if not findings_text:
        raise ValueError("findings_context must be a non-empty string")
    question_text = user_question.strip()
    if not question_text:
        raise ValueError("user_question must be a non-empty string")

    sections = [
        "You are an analyst assistant for Pension-Data findings interpretation.",
        "Explain findings with concise, evidence-based reasoning grounded in provided data.",
        "",
        "FINDINGS CONTEXT:",
        findings_text,
        "",
        "USER QUESTION:",
        question_text,
    ]
    context = (additional_context or "").strip()
    if context:
        sections.extend(["", "ADDITIONAL CONTEXT:", context])
    sections.extend(
        [
            "",
            "RESPONSE REQUIREMENTS:",
            "- Keep conclusions tied to the findings context.",
            "- State uncertainty where inputs are incomplete.",
            f"- End response with this exact disclaimer: {DEFAULT_ANALYTICS_DISCLAIMER}",
        ]
    )
    return "\n".join(sections).strip()
