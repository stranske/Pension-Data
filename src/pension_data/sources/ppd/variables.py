"""Centralized Public Plans Database (PPD) variable-name mapping table.

This module is the single source of truth for the PPD codebook variable names
that the ingest maps onto the repository's analytics dataclasses. Every mapping
function reads names from :data:`PPD_VARIABLES` (never a hard-coded literal), so
renaming a PPD variable is a **one-line change here** -- which is exactly what
the deliberate-break test in ``tests/sources/test_ppd_ingest.py`` toggles.

Provenance of the names below:

* ``CONFIDENT`` -- documented PPD codebook variables verified against the public
  codebook / issue #645 text (stable identifiers PPD has published for years).
* ``GUESS`` -- plausible PPD names chosen where the exact codebook spelling was
  uncertain at implementation time (egress is sandboxed, so the live codebook
  could not be pulled). These are the only names to re-verify against a live
  ``gettemplate&template=data-codebook`` pull; a rename stays a one-line edit.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Confidence = Literal["CONFIDENT", "GUESS"]


@dataclass(frozen=True, slots=True)
class PpdVariable:
    """One PPD codebook variable name plus its confidence + human note."""

    name: str
    confidence: Confidence
    note: str


# --- Identity + classification -------------------------------------------------
PPD_ID = PpdVariable("ppd_id", "CONFIDENT", "Stable PPD plan identifier.")
PLAN_NAME = PpdVariable("PlanName", "CONFIDENT", "Plan display name.")
FISCAL_YEAR = PpdVariable("fy", "CONFIDENT", "Plan fiscal year.")
STATE = PpdVariable("StateAbbrev", "CONFIDENT", "Two-letter state abbreviation.")
PLAN_TYPE = PpdVariable(
    "PlanType",
    "CONFIDENT",
    "Coded plan type: 1=general/mixed, 2=teachers, 3=police/fire/safety.",
)

# --- Funded status -------------------------------------------------------------
ACT_LIABILITIES = PpdVariable(
    "ActLiabilities_GASB", "CONFIDENT", "Actuarial accrued liability (GASB, AAL)."
)
ACT_ASSETS = PpdVariable("ActAssets_GASB", "CONFIDENT", "Actuarial value of assets (AVA).")
FUNDED_RATIO_AVA = PpdVariable(
    "ActFundedRatio_GASB", "CONFIDENT", "Actuarial-value funded ratio (AVA)."
)
MKT_ASSETS = PpdVariable(
    "MktAssets_ActRpt", "CONFIDENT", "Market value of assets as reported in the actuarial report."
)
FUNDED_RATIO_MVA = PpdVariable(
    "MktFundedRatio_ActRpt",
    "GUESS",
    "Market-value funded ratio. If absent in a live pull, the mapper derives it "
    "as MktAssets_ActRpt / ActLiabilities_GASB from CONFIDENT variables.",
)
UAAL = PpdVariable("UAAL_GASB", "CONFIDENT", "Unfunded actuarial accrued liability (GASB).")

# --- Cash flows ----------------------------------------------------------------
EMPLOYER_CONTRIB = PpdVariable(
    "EmployerContribution",
    "GUESS",
    "Employer (state/local) contribution in USD. Name per issue #645 guidance.",
)
EMPLOYEE_CONTRIB = PpdVariable(
    "EmployeeContribution",
    "GUESS",
    "Employee/member contribution in USD. Name per issue #645 guidance.",
)
BENEFIT_PAYMENTS = PpdVariable(
    "benefit_payments",
    "GUESS",
    "Total benefit payments / payout in USD. Name per issue #645 guidance.",
)

# --- Membership (used for maturity derivation) ---------------------------------
ACTIVE_MEMBERS = PpdVariable(
    "actives_tot", "GUESS", "Total active members; used for the maturity support ratio."
)
BENEFICIARIES = PpdVariable(
    "beneficiaries_tot", "GUESS", "Total beneficiaries; used for the maturity support ratio."
)

# --- Asset allocation (percent of portfolio) -----------------------------------
ALLOC_EQUITY = PpdVariable(
    "equities_actual", "GUESS", "Actual allocation to public equities (fraction or percent)."
)
ALLOC_FIXED_INCOME = PpdVariable(
    "FixedIncome_actual", "GUESS", "Actual allocation to fixed income."
)
ALLOC_REAL_ESTATE = PpdVariable("RealEstate_actual", "GUESS", "Actual allocation to real estate.")
ALLOC_ALTERNATIVES = PpdVariable(
    "alternatives_actual", "GUESS", "Actual allocation to alternatives/private markets."
)
ALLOC_CASH = PpdVariable("cash_actual", "GUESS", "Actual allocation to cash / short term.")


# Canonical registry keyed by a stable internal slug. Mapping code references
# variables through this table exclusively (see module docstring).
PPD_VARIABLES: dict[str, PpdVariable] = {
    "ppd_id": PPD_ID,
    "plan_name": PLAN_NAME,
    "fy": FISCAL_YEAR,
    "state": STATE,
    "plan_type": PLAN_TYPE,
    "act_liabilities": ACT_LIABILITIES,
    "act_assets": ACT_ASSETS,
    "funded_ratio_ava": FUNDED_RATIO_AVA,
    "mkt_assets": MKT_ASSETS,
    "funded_ratio_mva": FUNDED_RATIO_MVA,
    "uaal": UAAL,
    "employer_contrib": EMPLOYER_CONTRIB,
    "employee_contrib": EMPLOYEE_CONTRIB,
    "benefit_payments": BENEFIT_PAYMENTS,
    "active_members": ACTIVE_MEMBERS,
    "beneficiaries": BENEFICIARIES,
    "alloc_equity": ALLOC_EQUITY,
    "alloc_fixed_income": ALLOC_FIXED_INCOME,
    "alloc_real_estate": ALLOC_REAL_ESTATE,
    "alloc_alternatives": ALLOC_ALTERNATIVES,
    "alloc_cash": ALLOC_CASH,
}

# Asset-class label -> internal allocation slug, for allocation-by-class mapping.
ALLOCATION_CLASS_SLUGS: dict[str, str] = {
    "equity": "alloc_equity",
    "fixed_income": "alloc_fixed_income",
    "real_estate": "alloc_real_estate",
    "alternatives": "alloc_alternatives",
    "cash": "alloc_cash",
}


def variable_name(slug: str) -> str:
    """Return the live PPD variable name for an internal slug.

    Central indirection so a codebook rename is one edit in :data:`PPD_VARIABLES`.
    """
    return PPD_VARIABLES[slug].name


def request_variable_names() -> tuple[str, ...]:
    """Deterministic, de-duplicated list of PPD names to request from QVariables."""
    seen: dict[str, None] = {}
    for slug in PPD_VARIABLES:
        seen.setdefault(PPD_VARIABLES[slug].name, None)
    return tuple(seen)


def guessed_variable_names() -> tuple[tuple[str, str], ...]:
    """(slug, name) pairs whose codebook spelling is a GUESS -- for audit reporting."""
    return tuple(
        (slug, var.name) for slug, var in PPD_VARIABLES.items() if var.confidence == "GUESS"
    )
