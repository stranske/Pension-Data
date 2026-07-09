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

__all__ = ["is_finite_number", "require_finite", "finite_or_none"]


def is_finite_number(value: object) -> bool:
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
