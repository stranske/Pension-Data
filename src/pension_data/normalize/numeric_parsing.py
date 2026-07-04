"""Shared deterministic numeric-token parsing helpers."""

from __future__ import annotations

import re

from pension_data.normalize.financial_units import UnitScale

NUMBER_PATTERN = re.compile(r"[-+]?\d[\d,]*(?:\.\d+)?")
_BILLION_ABBREVIATION_PATTERN = re.compile(r"(?<![a-z])bn\b")
_MILLION_ABBREVIATION_PATTERN = re.compile(r"(?<![a-z])mm\b")
_THOUSAND_ABBREVIATION_PATTERN = re.compile(r"(?<![a-z])k\b")


def is_year_like_token(token: str) -> bool:
    """Return true for plain four-digit year tokens that should not be metric values."""
    cleaned = token.replace(",", "")
    return cleaned.isdigit() and len(cleaned) == 4 and cleaned.startswith(("19", "20"))


def parse_numeric_token(text: str, *, skip_year_like: bool = True) -> float | None:
    """Parse the first numeric token, optionally skipping standalone year-like tokens."""
    for match in NUMBER_PATTERN.finditer(text):
        token = match.group(0)
        if skip_year_like and is_year_like_token(token):
            continue
        return float(token.replace(",", ""))
    return None


def detect_money_scale(text: str, *, fallback: UnitScale) -> UnitScale:
    """Infer the money unit scale from nearby text."""
    lowered = text.lower()
    if "billion" in lowered or _BILLION_ABBREVIATION_PATTERN.search(lowered):
        return "billion_usd"
    if "million" in lowered or _MILLION_ABBREVIATION_PATTERN.search(lowered):
        return "million_usd"
    if "thousand" in lowered or _THOUSAND_ABBREVIATION_PATTERN.search(lowered):
        return "thousand_usd"
    return fallback
