"""Tests for API key permission scope definitions and operation mapping."""

from __future__ import annotations

import pytest

from pension_data.api.auth import (
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


def test_domain_scope_mapping_defines_required_domains() -> None:
    assert DOMAIN_SCOPES["query"] == SCOPE_QUERY
    assert DOMAIN_SCOPES["nl"] == SCOPE_NL
    assert DOMAIN_SCOPES["export"] == SCOPE_EXPORT
    assert DOMAIN_SCOPES["admin"] == SCOPE_ADMIN


def test_required_scope_for_operation_maps_known_domains() -> None:
    assert required_scope_for_operation("query.run") == SCOPE_QUERY
    assert required_scope_for_operation("nl.ask") == SCOPE_NL
    assert required_scope_for_operation("export.csv") == SCOPE_EXPORT
    assert required_scope_for_operation("admin.keys.rotate") == SCOPE_ADMIN


def test_required_scope_for_operation_rejects_unknown_domain() -> None:
    with pytest.raises(InvalidOperationError, match="unknown operation domain"):
        required_scope_for_operation("billing.invoice.create")


def test_required_scope_for_operation_rejects_empty_operation() -> None:
    with pytest.raises(InvalidOperationError, match="operation is required"):
        required_scope_for_operation("  ")


def test_normalize_scopes_sorts_and_deduplicates() -> None:
    normalized = normalize_scopes([SCOPE_EXPORT, SCOPE_QUERY, SCOPE_QUERY])
    assert normalized == (SCOPE_EXPORT, SCOPE_QUERY)


def test_normalize_scopes_rejects_unknown_scope() -> None:
    with pytest.raises(InvalidScopeError, match="unknown scopes"):
        normalize_scopes([SCOPE_QUERY, "unknown:scope"])


def test_normalize_scopes_rejects_empty_scope_set() -> None:
    with pytest.raises(InvalidScopeError, match="at least one scope is required"):
        normalize_scopes([])


def test_has_scope_allows_admin_global_access() -> None:
    assert has_scope(granted_scopes=(SCOPE_ADMIN,), required_scope=SCOPE_QUERY) is True


def test_has_scope_requires_direct_scope_for_non_admin_keys() -> None:
    assert has_scope(granted_scopes=(SCOPE_QUERY,), required_scope=SCOPE_NL) is False
