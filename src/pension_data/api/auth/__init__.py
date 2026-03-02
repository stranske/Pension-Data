"""API key auth helpers with scope enforcement and audit propagation."""

from pension_data.api.auth.audit import build_audit_event
from pension_data.api.auth.middleware import (
    AuthContext,
    InvalidAPIKeyError,
    MissingAPIKeyError,
    RevokedAPIKeyError,
    ScopeDeniedError,
    authenticate_request,
)
from pension_data.api.auth.scopes import (
    SCOPE_ADMIN,
    SCOPE_EXPORT,
    SCOPE_NL,
    SCOPE_QUERY,
)
from pension_data.api.auth.store import APIKeyStore

__all__ = [
    "APIKeyStore",
    "AuthContext",
    "InvalidAPIKeyError",
    "MissingAPIKeyError",
    "RevokedAPIKeyError",
    "SCOPE_ADMIN",
    "SCOPE_EXPORT",
    "SCOPE_NL",
    "SCOPE_QUERY",
    "ScopeDeniedError",
    "authenticate_request",
    "build_audit_event",
]
