"""Shared normalization helpers for entity IDs and aliases."""

from __future__ import annotations

import re


def normalize_entity_token(value: str | None) -> str:
    """Normalize free-text entity tokens to deterministic lowercase keys."""
    if value is None:
        return ""
    collapsed = re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()
    return " ".join(collapsed.split())
