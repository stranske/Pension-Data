"""API key auth helpers with scope enforcement and audit propagation."""

from pension_data.api.auth.audit import build_audit_event
from pension_data.api.auth.middleware import (
    AuthContext,
    AuthError,
    InvalidAPIKeyError,
    MissingAPIKeyError,
    RevokedAPIKeyError,
    ScopeDeniedError,
    authenticate_request,
)
from pension_data.api.auth.scopes import (
    DOMAIN_SCOPES,
    SCOPE_ADMIN,
    SCOPE_EXPORT,
    SCOPE_NL,
    SCOPE_QUERY,
    InvalidOperationError,
    InvalidScopeError,
    has_scope,
    normalize_scopes,
    required_scope_for_operation,
)
from pension_data.api.auth.store import APIKeyStore
from pension_data.api.auth.store import APIKeyInactiveError
from pension_data.api.auth.store import APIKeyLifecycleError
from pension_data.api.auth.store import APIKeyNotFoundError

__all__ = [
    "APIKeyStore",
    "APIKeyInactiveError",
    "APIKeyLifecycleError",
    "APIKeyNotFoundError",
    "DOMAIN_SCOPES",
    "AuthContext",
    "AuthError",
    "InvalidAPIKeyError",
    "InvalidOperationError",
    "InvalidScopeError",
    "MissingAPIKeyError",
    "RevokedAPIKeyError",
    "SCOPE_ADMIN",
    "SCOPE_EXPORT",
    "SCOPE_NL",
    "SCOPE_QUERY",
    "ScopeDeniedError",
    "authenticate_request",
    "build_audit_event",
    "has_scope",
    "normalize_scopes",
    "required_scope_for_operation",
]
