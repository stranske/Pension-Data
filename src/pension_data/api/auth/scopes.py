"""Permission scopes for API key authorization."""

from __future__ import annotations

SCOPE_QUERY = "query:read"
SCOPE_NL = "nl:query"
SCOPE_EXPORT = "export:read"
SCOPE_ADMIN = "admin:keys"

ALL_SCOPES: tuple[str, ...] = (
    SCOPE_QUERY,
    SCOPE_NL,
    SCOPE_EXPORT,
    SCOPE_ADMIN,
)


class InvalidScopeError(ValueError):
    """Raised when an unknown scope is requested or assigned."""


def normalize_scopes(scopes: tuple[str, ...] | list[str] | set[str]) -> tuple[str, ...]:
    """Normalize and validate scope sets into deterministic sorted tuples."""
    normalized = tuple(sorted(set(scopes)))
    invalid = [scope for scope in normalized if scope not in ALL_SCOPES]
    if invalid:
        raise InvalidScopeError(f"unknown scopes: {', '.join(invalid)}")
    return normalized


def has_scope(*, granted_scopes: tuple[str, ...], required_scope: str) -> bool:
    """Check whether a granted scope set satisfies the required operation scope."""
    if required_scope not in ALL_SCOPES:
        raise InvalidScopeError(f"unknown required scope: {required_scope}")
    if SCOPE_ADMIN in granted_scopes:
        return True
    return required_scope in granted_scopes
