"""Shared canonical entity ID helpers used across extractors."""

from __future__ import annotations

from pension_data.normalize.entity_tokens import normalize_entity_token


def canonical_manager_id(manager_name: str | None) -> str | None:
    """Build canonical manager ID from one manager name."""
    token = normalize_entity_token(manager_name)
    if not token:
        return None
    return f"manager:{token}"


def canonical_fund_id(*, manager_name: str | None, fund_name: str | None) -> str | None:
    """Build canonical fund ID from one manager/fund name pair."""
    fund_token = normalize_entity_token(fund_name)
    if not fund_token:
        return None
    manager_token = normalize_entity_token(manager_name)
    if manager_token:
        return f"fund:{manager_token}:{fund_token}"
    return f"fund:{fund_token}"
