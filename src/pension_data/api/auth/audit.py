"""Auth-context enrichment for request/audit trail events."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pension_data.api.auth.middleware import AuthContext

RESERVED_AUDIT_KEYS = frozenset({"operation", "api_key_id", "api_key_label", "api_key_scopes"})


def build_audit_event(
    *,
    operation: str,
    auth_context: AuthContext,
    event: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build an audit event payload that captures key identity context."""
    if event is not None:
        conflicting_keys = sorted(RESERVED_AUDIT_KEYS.intersection(event))
        if conflicting_keys:
            raise ValueError(
                "event contains reserved auth audit key(s): " + ", ".join(conflicting_keys)
            )

    payload: dict[str, Any] = dict(event or {})
    payload["operation"] = operation
    payload["api_key_id"] = auth_context.key_id
    payload["api_key_label"] = auth_context.label
    payload["api_key_scopes"] = list(auth_context.scopes)
    return payload
