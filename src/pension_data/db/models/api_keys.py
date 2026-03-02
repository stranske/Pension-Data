"""API key persistence model for authentication and authorization."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

APIKeyStatus = Literal["active", "revoked"]
APIKeyHashScheme = Literal["sha256"]
RevocationReason = Literal["manual", "rotated"]


@dataclass(frozen=True, slots=True)
class APIKeyRecord:
    """Stored API key metadata and authorization state."""

    key_id: str
    key_hash: str
    hash_scheme: APIKeyHashScheme
    scopes: tuple[str, ...]
    status: APIKeyStatus
    created_at: datetime
    label: str | None = None
    revoked_at: datetime | None = None
    revoked_reason: RevocationReason | None = None
    rotated_from: str | None = None
    rotated_to: str | None = None
