"""Registry domain models for pension system identity and cohorts."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PensionSystemRecord:
    """Canonical pension-system registry row."""

    stable_id: str
    legal_name: str
    short_name: str
    system_type: str
    jurisdiction: str
    jurisdiction_type: str
    identity_key: str
    in_state_employee_universe: bool
    in_sampled_50: bool
