"""Finite/bounds guards for numeric trust boundaries (issue #636).

Bare ``< 0`` / ``> 1`` comparisons let NaN and +/-inf through: NaN compares false to
everything, and +inf reads as "non-negative". For a data-integrity product a
non-finite extracted value must never be silently stored or auto-accepted. These
helpers make the finite check explicit and consistent across the extraction,
normalization, quant, and quality boundaries.

Two disposition styles are provided so callers can pick the right failure mode:
- ``require_finite`` raises (use where a non-finite value is a hard extraction error);
- ``finite_or_none`` returns ``None`` (use to flag-and-skip without storing NaN).
"""

from __future__ import annotations

import math
from typing import SupportsFloat, SupportsIndex, TypeGuard, cast

__all__ = [
    "bounded_confidence",
    "bounded_confidence_or_none",
    "bounded_confidence_or_zero",
    "is_finite_number",
    "require_finite",
    "finite_or_none",
]


def is_finite_number(value: object) -> TypeGuard[int | float]:
    """True only for a real, finite ``int``/``float`` (``bool`` excluded)."""
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def require_finite(value: float, *, field: str) -> float:
    """Return ``value`` as a float, or raise ``ValueError`` if it is None/NaN/inf."""
    if not is_finite_number(value):
        raise ValueError(f"{field} must be a finite number, got {value!r}")
    return float(value)


def finite_or_none(value: float | None) -> float | None:
    """Return the finite float, or ``None`` for None/NaN/inf (flag, don't store)."""
    if value is None:
        return None
    return float(value) if is_finite_number(value) else None


def bounded_confidence(value: float) -> float:
    """Clamp a finite confidence to [0, 1]; reject NaN and infinities."""
    return round(max(0.0, min(1.0, require_finite(value, field="confidence"))), 6)


def bounded_confidence_or_none(value: object) -> float | None:
    """Clamp a finite confidence or return ``None`` for an untrustworthy value.

    Confidence tokens originate in extractor payloads, where a finite numeric
    value can legitimately arrive as a string.  Preserve that established input
    contract while still rejecting booleans, malformed values, NaN, and
    infinities before routing or persistence can trust them.
    """
    if isinstance(value, bool):
        return None
    try:
        parsed = float(cast(str | SupportsFloat | SupportsIndex, value))
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return bounded_confidence(parsed)


def bounded_confidence_or_zero(value: object) -> float:
    """Clamp a finite confidence or downgrade an untrustworthy value to zero."""
    bounded = bounded_confidence_or_none(value)
    return 0.0 if bounded is None else bounded
