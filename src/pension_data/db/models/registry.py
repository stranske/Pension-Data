"""Registry domain models for pension system identity and cohorts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

PENSION_SYSTEM_SCHEMA_VERSION = "v1"
PENSION_SYSTEM_BASE_FIELDS: tuple[str, ...] = (
    "stable_id",
    "legal_name",
    "short_name",
    "system_type",
    "jurisdiction",
)
PENSION_SYSTEM_COHORT_FIELDS: tuple[str, ...] = (
    "jurisdiction_type",
    "identity_key",
    "in_state_employee_universe",
    "in_sampled_50",
)
PENSION_SYSTEM_SCHEMA_FIELDS: tuple[str, ...] = (
    *PENSION_SYSTEM_BASE_FIELDS,
    *PENSION_SYSTEM_COHORT_FIELDS,
)

SystemType = Literal["public-pension", "teacher-pension"]
JurisdictionType = Literal["state", "territory"]


@dataclass(frozen=True, slots=True)
class V1CohortMembership:
    """Explicit v1 cohort membership flags for a pension system."""

    in_state_employee_universe: bool
    in_sampled_50: bool


@dataclass(frozen=True, slots=True)
class PensionSystemRecord:
    """Canonical pension-system registry row."""

    stable_id: str
    legal_name: str
    short_name: str
    system_type: SystemType
    jurisdiction: str
    jurisdiction_type: JurisdictionType
    identity_key: str
    in_state_employee_universe: bool
    in_sampled_50: bool

    @property
    def cohort(self) -> V1CohortMembership:
        """Return normalized v1 cohort membership flags."""
        return V1CohortMembership(
            in_state_employee_universe=self.in_state_employee_universe,
            in_sampled_50=self.in_sampled_50,
        )
