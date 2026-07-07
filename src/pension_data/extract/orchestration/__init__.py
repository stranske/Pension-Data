"""Fallback orchestration helpers for extraction reliability."""

from stranske_pdf_extract.contract import EscalationEvent, ParserAttempt
from stranske_pdf_extract.orchestration import FallbackOutcome, ParserStage, run_fallback_chain

PARSER_FALLBACK_ORDER_BY_DOMAIN: dict[str, tuple[str, ...]] = {
    "funded": ("table_primary", "text_fallback", "full_fallback"),
    "actuarial": ("table_primary", "text_fallback", "full_fallback"),
    "investment": ("table_primary", "text_fallback", "full_fallback"),
}

__all__ = [
    "EscalationEvent",
    "FallbackOutcome",
    "PARSER_FALLBACK_ORDER_BY_DOMAIN",
    "ParserAttempt",
    "ParserStage",
    "run_fallback_chain",
]
