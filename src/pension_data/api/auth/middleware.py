"""Auth middleware primitives for API key verification and scope enforcement."""

from __future__ import annotations

from dataclasses import dataclass

from pension_data.api.auth.scopes import has_scope
from pension_data.api.auth.store import APIKeyStore


class AuthError(PermissionError):
    """Base auth error class for request rejection behavior."""


class MissingAPIKeyError(AuthError):
    """Raised when a request omits API key credentials."""


class InvalidAPIKeyError(AuthError):
    """Raised when a key cannot be resolved in storage."""


class RevokedAPIKeyError(AuthError):
    """Raised when a request uses a revoked API key."""


class ScopeDeniedError(AuthError):
    """Raised when a key lacks the required scope."""


@dataclass(frozen=True, slots=True)
class AuthContext:
    """Resolved auth context propagated to handlers and audit logging."""

    key_id: str
    scopes: tuple[str, ...]
    label: str | None = None


def _extract_secret(api_key_header: str | None) -> str:
    if api_key_header is None or not api_key_header.strip():
        raise MissingAPIKeyError("missing API key")
    token = api_key_header.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    if not token:
        raise MissingAPIKeyError("missing API key")
    return token


def authenticate_request(
    *,
    api_key_header: str | None,
    required_scope: str,
    key_store: APIKeyStore,
) -> AuthContext:
    """Verify API key and enforce required scope for a request."""
    secret = _extract_secret(api_key_header)
    record = key_store.get_by_secret(secret)
    if record is None:
        raise InvalidAPIKeyError("invalid API key")
    if record.status != "active":
        raise RevokedAPIKeyError("revoked API key")
    if not has_scope(granted_scopes=record.scopes, required_scope=required_scope):
        raise ScopeDeniedError(f"scope denied for operation requiring '{required_scope}'")
    return AuthContext(
        key_id=record.key_id,
        scopes=record.scopes,
        label=record.label,
    )
