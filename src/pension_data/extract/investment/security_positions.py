"""Security-level holdings extraction and CAFR coverage reconciliation."""

from __future__ import annotations

import csv
import math
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass
from io import StringIO

from pension_data.db.models.investment_positions import (
    HoldingsCoverageReport,
    PlanSecurityPosition,
    SecurityDisclosureState,
    SecurityPositionSource,
)


@dataclass(frozen=True, slots=True)
class SecurityPositionInput:
    """Raw security-level holding from a public source."""

    security_name: str | None
    cusip: str | None
    ticker: str | None
    shares: float | None
    market_value_usd: float | None
    asset_class: str
    source: SecurityPositionSource
    as_of: str
    provenance_ref: str
    disclosure_state: SecurityDisclosureState = "disclosed"
    manager_name: str | None = None
    fund_name: str | None = None
    confidence: float = 1.0


@dataclass(frozen=True, slots=True)
class AcfrAllocationInput:
    """ACFR total-plan anchor row used to label holdings coverage."""

    asset_class: str
    market_value_usd: float
    provenance_ref: str


def _text(element: ET.Element, tag_name: str) -> str | None:
    for child in element.iter():
        if child.tag.rsplit("}", 1)[-1] == tag_name and child.text:
            value = child.text.strip()
            if value:
                return value
    return None


def _float_or_none(value: str | None) -> float | None:
    if value is None:
        return None
    cleaned = value.strip().replace(",", "")
    if not cleaned:
        return None
    try:
        parsed = float(cleaned)
    except ValueError:
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _normalize_cusip(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = "".join(ch for ch in value.upper() if ch.isalnum())
    return normalized or None


def _security_id(*, cusip: str | None, ticker: str | None, security_name: str | None) -> str:
    if cusip:
        return f"cusip:{cusip}"
    if ticker:
        return f"ticker:{ticker.strip().upper()}"
    if security_name:
        return "name:" + " ".join(security_name.lower().split())
    return "unknown:security"


def parse_13f_information_table_xml(
    xml_text: str,
    *,
    as_of: str,
    provenance_ref: str,
    asset_class: str = "public_equity",
) -> list[SecurityPositionInput]:
    """Parse an EDGAR 13F information table XML payload.

    13F values are reported in thousands of dollars, so `market_value_usd`
    multiplies the XML `value` field by 1,000.
    """
    root = ET.fromstring(xml_text)
    rows: list[SecurityPositionInput] = []
    for info_table in root.iter():
        if info_table.tag.rsplit("}", 1)[-1] != "infoTable":
            continue
        security_name = _text(info_table, "nameOfIssuer")
        cusip = _normalize_cusip(_text(info_table, "cusip"))
        value_thousands = _float_or_none(_text(info_table, "value"))
        shares = _float_or_none(_text(info_table, "sshPrnamt"))
        rows.append(
            SecurityPositionInput(
                security_name=security_name,
                cusip=cusip,
                ticker=None,
                shares=shares,
                market_value_usd=(
                    round(value_thousands * 1000.0, 6) if value_thousands is not None else None
                ),
                asset_class=asset_class,
                source="13f",
                as_of=as_of,
                provenance_ref=provenance_ref,
            )
        )
    return rows


def load_own_holdings_csv(
    csv_text: str,
    *,
    as_of: str,
    provenance_ref: str,
    default_asset_class: str = "unknown",
) -> list[SecurityPositionInput]:
    """Load a public own-holdings CSV export into normalized inputs."""
    reader = csv.DictReader(StringIO(csv_text))
    rows: list[SecurityPositionInput] = []
    for row in reader:
        security_name = row.get("security_name") or row.get("name")
        cusip = _normalize_cusip(row.get("cusip"))
        ticker = row.get("ticker")
        rows.append(
            SecurityPositionInput(
                security_name=security_name.strip() if security_name else None,
                cusip=cusip,
                ticker=ticker.strip().upper() if ticker and ticker.strip() else None,
                shares=_float_or_none(row.get("shares")),
                market_value_usd=_float_or_none(row.get("market_value_usd")),
                asset_class=(row.get("asset_class") or default_asset_class).strip().lower(),
                source="own_holdings_file",
                as_of=as_of,
                provenance_ref=provenance_ref,
                manager_name=(row.get("manager_name") or None),
                fund_name=(row.get("fund_name") or None),
            )
        )
    return rows


def build_security_positions(
    *,
    plan_id: str,
    plan_period: str,
    rows: list[SecurityPositionInput],
) -> list[PlanSecurityPosition]:
    """Stage security-level positions with deterministic IDs and ordering."""
    positions = [
        PlanSecurityPosition(
            plan_id=plan_id,
            plan_period=plan_period,
            security_id=_security_id(
                cusip=row.cusip,
                ticker=row.ticker,
                security_name=row.security_name,
            ),
            security_name=row.security_name.strip() if row.security_name else None,
            cusip=row.cusip,
            ticker=row.ticker.strip().upper() if row.ticker else None,
            shares=row.shares,
            market_value_usd=row.market_value_usd,
            asset_class=row.asset_class.strip().lower(),
            source=row.source,
            as_of=row.as_of,
            disclosure_state=row.disclosure_state,
            provenance_ref=row.provenance_ref,
            manager_name=row.manager_name.strip() if row.manager_name else None,
            fund_name=row.fund_name.strip() if row.fund_name else None,
            confidence=max(0.0, min(1.0, row.confidence)),
        )
        for row in rows
    ]
    return sorted(
        positions,
        key=lambda row: (
            row.plan_id,
            row.plan_period,
            row.asset_class,
            row.security_id,
            row.provenance_ref,
        ),
    )


def reconcile_holdings_to_acfr(
    *,
    plan_id: str,
    plan_period: str,
    positions: list[PlanSecurityPosition],
    total_plan_assets_usd: float,
    acfr_allocations: list[AcfrAllocationInput],
) -> HoldingsCoverageReport:
    """Compute holdings coverage against ACFR total-plan asset values."""
    if total_plan_assets_usd <= 0.0 or not math.isfinite(total_plan_assets_usd):
        msg = "total_plan_assets_usd must be a positive finite value"
        raise ValueError(msg)

    collected_by_asset_class: dict[str, float] = defaultdict(float)
    for position in positions:
        if (
            position.plan_id != plan_id
            or position.plan_period != plan_period
            or position.disclosure_state != "disclosed"
            or position.market_value_usd is None
        ):
            continue
        collected_by_asset_class[position.asset_class] += position.market_value_usd

    acfr_by_asset_class = {
        row.asset_class.strip().lower(): row.market_value_usd for row in acfr_allocations
    }
    combined_classes = sorted(set(collected_by_asset_class) | set(acfr_by_asset_class))
    collected_total = round(sum(collected_by_asset_class.values()), 6)
    provenance_refs = tuple(
        dict.fromkeys(
            [
                *(position.provenance_ref for position in positions if position.provenance_ref),
                *(row.provenance_ref for row in acfr_allocations if row.provenance_ref),
            ]
        )
    )
    coverage_ratio = round(collected_total / total_plan_assets_usd, 6)
    scope_label = "total-plan" if coverage_ratio >= 0.95 else "equity-sleeve"

    return HoldingsCoverageReport(
        plan_id=plan_id,
        plan_period=plan_period,
        total_plan_assets_usd=round(total_plan_assets_usd, 6),
        collected_market_value_usd=collected_total,
        coverage_ratio=coverage_ratio,
        scope_label=scope_label,
        by_asset_class={
            asset_class: round(
                collected_by_asset_class.get(asset_class, 0.0) / acfr_by_asset_class[asset_class],
                6,
            )
            for asset_class in combined_classes
            if acfr_by_asset_class.get(asset_class, 0.0) > 0.0
        },
        provenance_refs=provenance_refs,
    )
