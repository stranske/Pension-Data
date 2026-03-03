"""Tests for extraction fallback orchestration and escalation control flow."""

from __future__ import annotations

from pension_data.extract.orchestration.fallback import (
    PARSER_FALLBACK_ORDER_BY_DOMAIN,
    ParserStage,
    run_fallback_chain,
)


def test_domain_fallback_order_is_defined_for_all_extraction_domains() -> None:
    assert set(PARSER_FALLBACK_ORDER_BY_DOMAIN) == {"funded", "actuarial", "investment"}
    assert all(
        PARSER_FALLBACK_ORDER_BY_DOMAIN[domain] for domain in PARSER_FALLBACK_ORDER_BY_DOMAIN
    )


def test_fallback_chain_uses_retry_order_until_result_is_complete() -> None:
    attempts: list[str] = []

    outcome = run_fallback_chain(
        domain="funded",
        stages=[
            ParserStage(
                stage_name="primary",
                parser_name="parser-primary",
                parse=lambda: attempts.append("primary") or {"complete": False},
            ),
            ParserStage(
                stage_name="fallback",
                parser_name="parser-fallback",
                parse=lambda: attempts.append("fallback") or {"complete": True},
            ),
        ],
        is_complete=lambda result: bool(result["complete"]),
    )

    assert attempts == ["primary", "fallback"]
    assert outcome.result == {"complete": True}
    assert outcome.escalation is None
    assert [attempt.succeeded for attempt in outcome.attempts] == [False, True]


def test_fallback_chain_emits_structured_escalation_when_exhausted() -> None:
    outcome = run_fallback_chain(
        domain="investment",
        stages=[
            ParserStage(
                stage_name="primary",
                parser_name="parser-primary",
                parse=lambda: {"complete": False},
            ),
            ParserStage(
                stage_name="fallback",
                parser_name="parser-fallback",
                parse=lambda: (_ for _ in ()).throw(RuntimeError("failed parser stage")),
            ),
        ],
        is_complete=lambda result: bool(result["complete"]),
    )

    assert outcome.result is None
    assert outcome.escalation is not None
    assert outcome.escalation.reason == "parser_fallback_exhaustion"
    assert outcome.escalation.exhausted_stage_count == 2
    assert len(outcome.escalation.attempts) == 2
