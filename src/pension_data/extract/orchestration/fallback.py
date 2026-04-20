"""Retry/fallback orchestration for extraction parser chains."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

PARSER_FALLBACK_ORDER_BY_DOMAIN: dict[str, tuple[str, ...]] = {
    "funded": ("table_primary", "text_fallback", "full_fallback"),
    "actuarial": ("table_primary", "text_fallback", "full_fallback"),
    "investment": ("table_primary", "text_fallback", "full_fallback"),
}


@dataclass(frozen=True, slots=True)
class ParserAttempt:
    """Structured record for one parser stage attempt."""

    stage_name: str
    parser_name: str
    succeeded: bool
    failure_reason: str | None = None


@dataclass(frozen=True, slots=True)
class EscalationEvent:
    """Escalation payload emitted when all fallback stages fail."""

    domain: str
    reason: str
    exhausted_stage_count: int
    attempts: tuple[ParserAttempt, ...]


@dataclass(frozen=True, slots=True)
class ParserStage[TResult]:
    """One parser stage in a domain fallback chain."""

    stage_name: str
    parser_name: str
    parse: Callable[[], TResult]


@dataclass(frozen=True, slots=True)
class FallbackOutcome[TResult]:
    """Output for fallback orchestration."""

    result: TResult | None
    attempts: tuple[ParserAttempt, ...]
    escalation: EscalationEvent | None


def run_fallback_chain[TResult](
    *,
    domain: str,
    stages: Sequence[ParserStage[TResult]],
    is_complete: Callable[[TResult], bool],
) -> FallbackOutcome[TResult]:
    """Execute parser stages in order and escalate when chain exhausts."""
    attempts: list[ParserAttempt] = []
    for stage in stages:
        try:
            parsed = stage.parse()
        except Exception as exc:  # noqa: BLE001 - needed for structured failure path
            attempts.append(
                ParserAttempt(
                    stage_name=stage.stage_name,
                    parser_name=stage.parser_name,
                    succeeded=False,
                    failure_reason=f"exception:{type(exc).__name__}:{exc}",
                )
            )
            continue

        if not is_complete(parsed):
            attempts.append(
                ParserAttempt(
                    stage_name=stage.stage_name,
                    parser_name=stage.parser_name,
                    succeeded=False,
                    failure_reason="incomplete-required-fields",
                )
            )
            continue

        attempts.append(
            ParserAttempt(
                stage_name=stage.stage_name,
                parser_name=stage.parser_name,
                succeeded=True,
            )
        )
        return FallbackOutcome(
            result=parsed,
            attempts=tuple(attempts),
            escalation=None,
        )

    escalation = EscalationEvent(
        domain=domain,
        reason="parser_fallback_exhaustion",
        exhausted_stage_count=len(stages),
        attempts=tuple(attempts),
    )
    return FallbackOutcome(
        result=None,
        attempts=tuple(attempts),
        escalation=escalation,
    )
