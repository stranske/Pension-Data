"""Inventory domain models for discovery and annual-report coverage outputs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

from pension_data.sources.schema import OfficialResolutionState, SourceAuthorityTier

InventoryDocumentType = Literal[
    "annual_report",
    "board_packet",
    "alm_study",
    "consultant_report",
    "other",
]


@dataclass(frozen=True, slots=True)
class DiscoveredInventoryRecord:
    """Discovered document record with inventory classification metadata."""

    plan_id: str
    plan_year: int | None
    document_type: InventoryDocumentType
    source_url: str
    source_authority_tier: SourceAuthorityTier
    manager_disclosure_available: bool
    consultant_disclosure_available: bool
    detection_metadata: Mapping[str, str] = field(compare=False, hash=False)


@dataclass(frozen=True, slots=True)
class AnnualReportCoverageRecord:
    """Plan-year annual-report availability state with inventory disclosure flags."""

    plan_id: str
    plan_year: int
    cohort: str
    system_type: str
    official_resolution_state: OfficialResolutionState
    annual_report_source_url: str
    manager_disclosure_available: bool
    consultant_disclosure_available: bool
