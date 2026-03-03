"""Deterministic ID helpers shared by extraction/provenance modules."""

from __future__ import annotations

import hashlib
import json


def stable_id(prefix: str, *parts: object) -> str:
    """Build a short deterministic identifier from typed JSON-compatible parts."""
    encoded_parts = [json.dumps(part, sort_keys=True) for part in parts]
    digest = hashlib.sha256("|".join(encoded_parts).encode("utf-8")).hexdigest()[:20]
    return f"{prefix}:{digest}"
