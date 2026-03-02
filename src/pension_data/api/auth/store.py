"""In-memory API key store with lifecycle operations."""

from __future__ import annotations

import hashlib
import secrets
from datetime import UTC, datetime

from pension_data.api.auth.scopes import normalize_scopes
from pension_data.db.models.api_keys import APIKeyRecord


class APIKeyNotFoundError(KeyError):
    """Raised when a key ID does not exist in storage."""


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _hash_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


class APIKeyStore:
    """Simple API key store abstraction for auth middleware and tests."""

    def __init__(self) -> None:
        self._records_by_id: dict[str, APIKeyRecord] = {}
        self._hash_to_id: dict[str, str] = {}

    def create_key(
        self,
        *,
        scopes: tuple[str, ...] | list[str] | set[str],
        label: str | None = None,
    ) -> tuple[str, APIKeyRecord]:
        """Create a new API key and return the plaintext key once."""
        normalized_scopes = normalize_scopes(scopes)
        key_id = f"key_{secrets.token_hex(8)}"
        secret = f"pd_{secrets.token_urlsafe(24)}"
        key_hash = _hash_secret(secret)
        record = APIKeyRecord(
            key_id=key_id,
            key_hash=key_hash,
            scopes=normalized_scopes,
            status="active",
            created_at=_utcnow(),
            label=label,
        )
        self._records_by_id[key_id] = record
        self._hash_to_id[key_hash] = key_id
        return secret, record

    def get_by_secret(self, secret: str) -> APIKeyRecord | None:
        """Resolve API key secret to stored metadata record."""
        key_hash = _hash_secret(secret)
        key_id = self._hash_to_id.get(key_hash)
        if key_id is None:
            return None
        return self._records_by_id.get(key_id)

    def get_by_id(self, key_id: str) -> APIKeyRecord:
        """Return API key record by ID or raise if missing."""
        if key_id not in self._records_by_id:
            raise APIKeyNotFoundError(key_id)
        return self._records_by_id[key_id]

    def revoke_key(self, key_id: str) -> APIKeyRecord:
        """Revoke an active key; this action is idempotent."""
        record = self.get_by_id(key_id)
        if record.status == "revoked":
            return record
        revoked = APIKeyRecord(
            key_id=record.key_id,
            key_hash=record.key_hash,
            scopes=record.scopes,
            status="revoked",
            created_at=record.created_at,
            label=record.label,
            revoked_at=_utcnow(),
            rotated_from=record.rotated_from,
        )
        self._records_by_id[key_id] = revoked
        return revoked

    def rotate_key(
        self,
        key_id: str,
        *,
        scopes: tuple[str, ...] | list[str] | set[str] | None = None,
        label: str | None = None,
    ) -> tuple[str, APIKeyRecord]:
        """Rotate key material while preserving historical linkage metadata."""
        existing = self.get_by_id(key_id)
        self.revoke_key(key_id)
        next_scopes = scopes if scopes is not None else existing.scopes
        next_label = label if label is not None else existing.label
        secret, created = self.create_key(scopes=next_scopes, label=next_label)
        rotated = APIKeyRecord(
            key_id=created.key_id,
            key_hash=created.key_hash,
            scopes=created.scopes,
            status=created.status,
            created_at=created.created_at,
            label=created.label,
            rotated_from=key_id,
        )
        self._records_by_id[rotated.key_id] = rotated
        return secret, rotated
