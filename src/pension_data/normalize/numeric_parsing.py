"""Shared deterministic numeric-token parsing helpers."""

from __future__ import annotations

import re

from pension_data.normalize.financial_units import UnitScale

# Capture numbers using EITHER separator so European ("1.234,56") and US ("1,234.56")
# groupings both match; locale disambiguation happens in _token_to_float.
NUMBER_PATTERN = re.compile(r"[-+]?\d[\d.,]*\d|[-+]?\d")
_BILLION_ABBREVIATION_PATTERN = re.compile(r"(?<![a-z])bn\b")
_MILLION_ABBREVIATION_PATTERN = re.compile(r"(?<![a-z])mm\b")
_THOUSAND_ABBREVIATION_PATTERN = re.compile(r"(?<![a-z])k\b")


def is_year_like_token(token: str) -> bool:
    """Return true for plain four-digit year tokens that should not be metric values."""
    cleaned = token.replace(",", "").replace(".", "")
    return cleaned.isdigit() and len(cleaned) == 4 and cleaned.startswith(("19", "20"))


def _token_to_float(token: str) -> float | None:
    """Parse one numeric token, inferring US vs European separator convention.

    - both `.` and `,`: the RIGHTMOST is the decimal separator, the other is grouping.
    - only `,`: decimal when a single group with 1-2 trailing digits ("81,2"), else grouping.
    - only `.`: decimal, EXCEPT a single "...NNN" group of three trailing ZEROS is grouping
      ("325.000" -> 325000) so European thousands don't collapse to a fraction; a genuine
      three-decimal value ("7.125") stays a decimal.
    """
    sign = -1.0 if token.strip().startswith("-") else 1.0
    body = token.strip().lstrip("+-")
    has_dot = "." in body
    has_comma = "," in body
    try:
        if has_dot and has_comma:
            if body.rfind(".") > body.rfind(","):
                cleaned = body.replace(",", "")
            else:
                cleaned = body.replace(".", "").replace(",", ".")
        elif has_comma:
            parts = body.split(",")
            if len(parts) == 2 and 1 <= len(parts[1]) <= 2:
                cleaned = body.replace(",", ".")
            else:
                cleaned = body.replace(",", "")
        elif has_dot:
            parts = body.split(".")
            if len(parts) > 2:
                cleaned = body.replace(".", "")  # multiple dots = European grouping
            elif len(parts) == 2 and parts[1] == "000":
                cleaned = body.replace(".", "")  # "325.000" thousands, not a fraction
            else:
                cleaned = body  # single dot with a real fraction ("78.4", "7.125")
        else:
            cleaned = body
        return sign * float(cleaned)
    except ValueError:
        return None


def parse_numeric_token(text: str, *, skip_year_like: bool = True) -> float | None:
    """Parse the first numeric token, optionally skipping standalone year-like tokens."""
    for match in NUMBER_PATTERN.finditer(text):
        token = match.group(0)
        if skip_year_like and is_year_like_token(token):
            continue
        value = _token_to_float(token)
        if value is not None:
            return value
    return None


_SENTENCE_BOUNDARY_PATTERN = re.compile(r"[.;!?](?:\s|$)")


def truncate_at_sentence_boundary(text: str) -> str:
    """Cut text at the first sentence terminator (`. `/`; `/newline/end).

    Keeps an alias-anchored value search from crossing into the next sentence — e.g.
    "Funded ratio not disclosed. AAL was $410.5 million." must not let the AAL figure
    become the funded ratio. A period inside a number ("78.4") is not a boundary
    because it is not followed by whitespace.
    """
    match = _SENTENCE_BOUNDARY_PATTERN.search(text)
    return text[: match.start()] if match else text


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
