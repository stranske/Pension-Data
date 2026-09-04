"""Map raw PPD records onto the repository's analytics input dataclasses.

All PPD variable names are read through :mod:`pension_data.sources.ppd.variables`
so a codebook rename is a one-line change there (and is what the deliberate-break
test toggles). Numeric values pass through the shared finite guard so a NaN/inf in
the source never crosses into the analytics inputs.
"""

from __future__ import annotations

from pension_data.finite_guards import is_finite_number
from pension_data.normalize.ratio_normalization import ppd_funded_ratio_fraction, to_percent
from pension_data.query.saved_views.models import (
    AllocationPeerInput,
    BenchmarkPanelInput,
    FundingTrendInput,
)
from pension_data.sources.ppd.variables import (
    ALLOCATION_CLASS_SLUGS,
    PPD_VARIABLES,
    variable_name,
)

# Plan-type codes as published in the PPD codebook (PlanType).
_PLAN_TYPE_LABELS: dict[str, str] = {
    "1": "general",
    "2": "teacher",
    "3": "safety",
}

# Total-asset thresholds (USD) for the size cohort. Market value of assets.
SIZE_SMALL_MAX_USD = 2_000_000_000.0
SIZE_LARGE_MIN_USD = 20_000_000_000.0

# Support ratio (beneficiaries / actives) above which a plan is treated as mature;
# used only when membership counts are present. Otherwise liability-to-asset is used.
MATURITY_SUPPORT_RATIO_THRESHOLD = 1.0
MATURITY_LIABILITY_TO_ASSET_THRESHOLD = 1.10


def _coerce_float(value: object) -> float | None:
    """Coerce a raw PPD cell to a finite float, or ``None``."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if is_finite_number(value) else None
    if isinstance(value, str):
        text = value.strip().replace(",", "")
        if not text or text.lower() in {"na", "n/a", "null", "none", "."}:
            return None
        try:
            parsed = float(text)
        except ValueError:
            return None
        return parsed if is_finite_number(parsed) else None
    return None


def _num(record: dict[str, object], slug: str) -> float | None:
    """Read one numeric PPD variable (by internal slug) from a record."""
    return _coerce_float(record.get(variable_name(slug)))


def _str(record: dict[str, object], slug: str) -> str | None:
    raw = record.get(variable_name(slug))
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


def ppd_id_of(record: dict[str, object]) -> str:
    """Return the record's ``ppd_id`` as a string, or raise if missing."""
    value = _str(record, "ppd_id")
    if value is None:
        raise KeyError(f"record missing required {variable_name('ppd_id')}")
    # PPD ids come back as ints in JSON; normalize "123.0" -> "123".
    numeric = _coerce_float(record.get(variable_name("ppd_id")))
    if numeric is not None and numeric.is_integer():
        return str(int(numeric))
    return value


def fiscal_year_of(record: dict[str, object]) -> int:
    """Return the record's fiscal year as an int, or raise if missing/invalid."""
    value = _num(record, "fy")
    if value is None or not value.is_integer():
        raise KeyError(f"record missing/invalid {variable_name('fy')}")
    return int(value)


def _as_fraction(value: float | None) -> float | None:
    """Normalize a funded ratio to a fraction (percent inputs >3 are divided by 100)."""
    return ppd_funded_ratio_fraction(value)


def _funded_ratio_mva(record: dict[str, object]) -> float | None:
    """Market-value funded ratio: direct variable if present, else MktAssets/AAL."""
    direct = _as_fraction(_num(record, "funded_ratio_mva"))
    if direct is not None:
        return direct
    mkt_assets = _num(record, "mkt_assets")
    liabilities = _num(record, "act_liabilities")
    if mkt_assets is not None and liabilities is not None and liabilities != 0.0:
        return mkt_assets / liabilities
    return None


def to_funding_trend_input(record: dict[str, object]) -> FundingTrendInput:
    """Map one PPD record to a :class:`FundingTrendInput` row."""
    funded_ratio = _as_fraction(_num(record, "funded_ratio_ava"))
    if funded_ratio is None:
        funded_ratio = _funded_ratio_mva(record)
    return FundingTrendInput(
        plan_id=ppd_id_of(record),
        plan_period=str(fiscal_year_of(record)),
        funded_ratio=funded_ratio if funded_ratio is not None else 0.0,
        employer_contributions_usd=_num(record, "employer_contrib"),
        employee_contributions_usd=_num(record, "employee_contrib"),
        benefit_payments_usd=_num(record, "benefit_payments"),
    )


def _allocation_percent(value: float | None) -> float | None:
    """Normalize an allocation weight to a 0-100 percent (fractions <=1 scaled up)."""
    return to_percent(value)


def to_allocation_peer_inputs(record: dict[str, object]) -> list[AllocationPeerInput]:
    """Map one PPD record to per-asset-class :class:`AllocationPeerInput` rows."""
    plan_id = ppd_id_of(record)
    plan_period = str(fiscal_year_of(record))
    peer_group = derive_peer_group(record)
    rows: list[AllocationPeerInput] = []
    for asset_class, slug in ALLOCATION_CLASS_SLUGS.items():
        pct = _allocation_percent(_num(record, slug))
        if pct is None:
            continue
        rows.append(
            AllocationPeerInput(
                plan_id=plan_id,
                plan_period=plan_period,
                peer_group=peer_group,
                asset_class=asset_class,
                allocation_pct=pct,
            )
        )
    return rows


def to_benchmark_panel_input(record: dict[str, object]) -> BenchmarkPanelInput:
    """Map one PPD record to a :class:`BenchmarkPanelInput` row (funded/AAL/UAAL)."""
    return BenchmarkPanelInput(
        plan_id=ppd_id_of(record),
        plan_period=str(fiscal_year_of(record)),
        peer_group=derive_peer_group(record),
        funded_ratio_ava=_as_fraction(_num(record, "funded_ratio_ava")),
        funded_ratio_mva=_funded_ratio_mva(record),
        aal_usd=_num(record, "act_liabilities"),
        uaal_usd=_num(record, "uaal"),
    )


# --- Peer-group derivation -----------------------------------------------------


def derive_plan_type(record: dict[str, object]) -> str:
    """Classify plan type from the coded PPD ``PlanType`` variable."""
    raw = _num(record, "plan_type")
    if raw is not None and raw.is_integer():
        return _PLAN_TYPE_LABELS.get(str(int(raw)), "unknown")
    text = _str(record, "plan_type")
    if text is None:
        return "unknown"
    lowered = text.lower()
    if "teach" in lowered:
        return "teacher"
    if any(token in lowered for token in ("police", "fire", "safety")):
        return "safety"
    if "general" in lowered or "employ" in lowered:
        return "general"
    return "unknown"


def derive_size(record: dict[str, object]) -> str:
    """Classify plan size from total market assets into small/medium/large."""
    assets = _num(record, "mkt_assets")
    if assets is None:
        assets = _num(record, "act_assets")
    if assets is None:
        return "unknown"
    if assets < SIZE_SMALL_MAX_USD:
        return "small"
    if assets >= SIZE_LARGE_MIN_USD:
        return "large"
    return "medium"


def derive_state(record: dict[str, object]) -> str:
    """Return the plan's state cohort (uppercased abbreviation) or ``unknown``."""
    state = _str(record, "state")
    return state.upper() if state else "unknown"


def derive_maturity(record: dict[str, object]) -> str:
    """Classify plan maturity as mature/growing.

    Prefers the demographic support ratio (beneficiaries / actives); falls back to
    the liability-to-asset ratio when membership counts are absent.
    """
    actives = _num(record, "active_members")
    beneficiaries = _num(record, "beneficiaries")
    if actives is not None and actives > 0.0 and beneficiaries is not None:
        ratio = beneficiaries / actives
        return "mature" if ratio >= MATURITY_SUPPORT_RATIO_THRESHOLD else "growing"

    liabilities = _num(record, "act_liabilities")
    assets = _num(record, "act_assets")
    if liabilities is not None and assets is not None and assets > 0.0:
        ratio = liabilities / assets
        return "mature" if ratio >= MATURITY_LIABILITY_TO_ASSET_THRESHOLD else "growing"
    return "unknown"


def derive_peer_group(record: dict[str, object]) -> str:
    """Derive the canonical peer-group label used by the saved views.

    The canonical cohort is ``<type>:<size>`` -- coarse enough that real cohorts
    have multiple members for peer statistics. The finer state/maturity axes are
    persisted alongside on the peer-universe row for tighter comparisons.
    """
    return f"{derive_plan_type(record)}:{derive_size(record)}"


def derive_peer_group_components(record: dict[str, object]) -> dict[str, str]:
    """Return all four peer-group axes for one record."""
    return {
        "plan_type": derive_plan_type(record),
        "size": derive_size(record),
        "state": derive_state(record),
        "maturity": derive_maturity(record),
    }


# Report the mapped fields so mapping coverage is explicit (used by tests).
MAPPED_VARIABLE_SLUGS: tuple[str, ...] = tuple(PPD_VARIABLES)
