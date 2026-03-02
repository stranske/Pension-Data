"""Auth tests for API key validation, scope enforcement, and audit context."""

from __future__ import annotations

import pytest

from pension_data.api.auth import (
    SCOPE_ADMIN,
    SCOPE_EXPORT,
    SCOPE_QUERY,
    APIKeyStore,
    AuthError,
    InvalidAPIKeyError,
    MissingAPIKeyError,
    RevokedAPIKeyError,
    ScopeDeniedError,
    authenticate_request,
    build_audit_event,
)


def test_missing_key_is_rejected() -> None:
    store = APIKeyStore()
    with pytest.raises(MissingAPIKeyError):
        authenticate_request(
            api_key_header=None,
            required_scope=SCOPE_QUERY,
            key_store=store,
        )


def test_invalid_key_is_rejected() -> None:
    store = APIKeyStore()
    with pytest.raises(InvalidAPIKeyError):
        authenticate_request(
            api_key_header="pd_not-a-real-key",
            required_scope=SCOPE_QUERY,
            key_store=store,
        )


def test_revoked_key_is_rejected() -> None:
    store = APIKeyStore()
    secret, record = store.create_key(scopes=(SCOPE_QUERY,), label="integration-key")
    store.revoke_key(record.key_id)
    with pytest.raises(RevokedAPIKeyError):
        authenticate_request(
            api_key_header=secret,
            required_scope=SCOPE_QUERY,
            key_store=store,
        )


def test_scope_denial_blocks_unauthorized_access() -> None:
    store = APIKeyStore()
    secret, _ = store.create_key(scopes=(SCOPE_QUERY,))
    with pytest.raises(ScopeDeniedError):
        authenticate_request(
            api_key_header=secret,
            required_scope=SCOPE_EXPORT,
            key_store=store,
        )


def test_scope_success_accepts_bearer_header() -> None:
    store = APIKeyStore()
    secret, record = store.create_key(scopes=(SCOPE_QUERY,))
    context = authenticate_request(
        api_key_header=f"Bearer {secret}",
        required_scope=SCOPE_QUERY,
        key_store=store,
    )
    assert context.key_id == record.key_id
    assert context.scopes == (SCOPE_QUERY,)
    assert record.hash_scheme == "sha256"
    assert record.key_hash.startswith("sha256:")
    assert secret not in record.key_hash


def test_admin_scope_grants_export_access() -> None:
    store = APIKeyStore()
    secret, _ = store.create_key(scopes=(SCOPE_ADMIN,))
    context = authenticate_request(
        api_key_header=secret,
        required_scope=SCOPE_EXPORT,
        key_store=store,
    )
    assert SCOPE_ADMIN in context.scopes


def test_key_rotation_updates_metadata_and_revokes_previous_key() -> None:
    store = APIKeyStore()
    first_secret, first_record = store.create_key(scopes=(SCOPE_QUERY,), label="first")
    second_secret, rotated = store.rotate_key(
        first_record.key_id,
        scopes=(SCOPE_EXPORT,),
        label="second",
    )

    assert rotated.rotated_from == first_record.key_id
    previous = store.get_by_id(first_record.key_id)
    assert previous.status == "revoked"
    assert previous.revoked_reason == "rotated"
    assert previous.rotated_to == rotated.key_id
    with pytest.raises(RevokedAPIKeyError):
        authenticate_request(
            api_key_header=first_secret,
            required_scope=SCOPE_QUERY,
            key_store=store,
        )

    context = authenticate_request(
        api_key_header=second_secret,
        required_scope=SCOPE_EXPORT,
        key_store=store,
    )
    assert context.key_id == rotated.key_id
    assert context.label == "second"


def test_key_rotation_allows_explicit_label_clear() -> None:
    store = APIKeyStore()
    _, first_record = store.create_key(scopes=(SCOPE_QUERY,), label="first")
    _, rotated = store.rotate_key(first_record.key_id, label=None)
    assert rotated.label is None


def test_auth_error_base_class_is_exported() -> None:
    store = APIKeyStore()
    with pytest.raises(AuthError):
        authenticate_request(
            api_key_header=None,
            required_scope=SCOPE_QUERY,
            key_store=store,
        )


def test_audit_event_captures_key_identity_context() -> None:
    store = APIKeyStore()
    secret, _ = store.create_key(scopes=(SCOPE_QUERY,), label="analyst-app")
    context = authenticate_request(
        api_key_header=secret,
        required_scope=SCOPE_QUERY,
        key_store=store,
    )
    event = build_audit_event(
        operation="query.run",
        auth_context=context,
        event={"query_id": "q-123"},
    )
    assert event["query_id"] == "q-123"
    assert event["operation"] == "query.run"
    assert event["api_key_id"] == context.key_id
    assert event["api_key_label"] == "analyst-app"
    assert event["api_key_scopes"] == [SCOPE_QUERY]


def test_audit_event_rejects_reserved_key_collisions() -> None:
    store = APIKeyStore()
    secret, _ = store.create_key(scopes=(SCOPE_QUERY,), label="analyst-app")
    context = authenticate_request(
        api_key_header=secret,
        required_scope=SCOPE_QUERY,
        key_store=store,
    )
    with pytest.raises(ValueError, match="reserved auth audit key"):
        build_audit_event(
            operation="query.run",
            auth_context=context,
            event={"operation": "existing"},
        )
