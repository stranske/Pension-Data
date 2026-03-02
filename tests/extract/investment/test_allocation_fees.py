"""Tests for investment allocation and fee extraction paths."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pension_data.extract.investment.allocation_fees import (
    AllocationDisclosureInput,
    FeeDisclosureInput,
    extract_asset_allocations,
    extract_fee_observations,
)
from pension_data.extract.investment.manager_positions import (
    ManagerFundDisclosureInput,
    build_manager_fund_positions,
)

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "allocation_fee_golden.json"


def _load_fixture() -> dict[str, Any]:
    with FIXTURE_PATH.open(encoding="utf-8") as handle:
        return json.load(handle)


def test_allocation_parser_extracts_percent_and_nominal_values() -> None:
    fixture = _load_fixture()["allocation_table_and_amount_mix"]
    allocations = extract_asset_allocations(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        effective_date=fixture["effective_date"],
        ingestion_date=fixture["ingestion_date"],
        source_document_id=fixture["source_document_id"],
        rows=[
            AllocationDisclosureInput(
                category_label=row["category_label"],
                percent_value=row["percent_value"],
                amount_value=row["amount_value"],
                amount_unit=row["amount_unit"],
                evidence_refs=tuple(row["evidence_refs"]),
            )
            for row in fixture["allocation_rows"]
        ],
    )

    assert len(allocations) == 4
    rows = {row.category: row for row in allocations}
    assert rows["public_equity"].normalized_weight == 0.425
    assert rows["public_equity"].normalized_amount_usd == 425_000_000.0
    assert rows["private_equity"].normalized_amount_usd == 175_000_000.0
    assert rows["cash"].normalized_weight == 0.1


def test_fee_parser_extracts_rows_and_emits_partial_ambiguous_and_nondisclosure_warnings() -> None:
    fixture = _load_fixture()["fee_schedule_partial_and_ambiguous"]
    fees, warnings = extract_fee_observations(
        plan_id=fixture["plan_id"],
        plan_period=fixture["plan_period"],
        effective_date=fixture["effective_date"],
        ingestion_date=fixture["ingestion_date"],
        source_document_id=fixture["source_document_id"],
        rows=[
            FeeDisclosureInput(
                manager_name=row["manager_name"],
                fee_type=row["fee_type"],
                rate_value=row["rate_value"],
                amount_value=row["amount_value"],
                amount_unit=row["amount_unit"],
                explicit_not_disclosed=row["explicit_not_disclosed"],
                evidence_refs=tuple(row["evidence_refs"]),
            )
            for row in fixture["fee_rows"]
        ],
    )

    assert len(fees) == 3
    alpha_management = [
        row
        for row in fees
        if row.manager_name == "Alpha Capital" and row.fee_type == "management_fee"
    ][0]
    assert alpha_management.normalized_rate == 0.015
    assert alpha_management.normalized_amount_usd == 18_000_000.0
    assert alpha_management.completeness == "complete"

    partial_row = [row for row in fees if row.fee_type == "performance_fee"][0]
    assert partial_row.completeness == "partial"

    nondisclosed_row = [row for row in fees if row.manager_name == "Beta Partners"][0]
    assert nondisclosed_row.completeness == "not_disclosed"

    warning_codes = [warning.code for warning in warnings]
    assert warning_codes.count("ambiguous_manager_name") == 2
    assert "partial_fee_disclosure" in warning_codes
    assert "non_disclosure" in warning_codes


def test_manager_level_holdings_parser_still_persists_non_security_rows() -> None:
    positions, warnings = build_manager_fund_positions(
        [
            ManagerFundDisclosureInput(
                plan_id="CA-PERS",
                plan_period="FY2025",
                manager_name="Alpha Capital",
                fund_name="Fund I",
                commitment=110.0,
                unfunded=22.0,
                market_value=95.0,
                confidence=0.94,
                evidence_refs=("p55",),
            ),
            ManagerFundDisclosureInput(
                plan_id="CA-PERS",
                plan_period="FY2025",
                manager_name=None,
                fund_name=None,
                commitment=None,
                unfunded=None,
                market_value=None,
                explicit_not_disclosed=True,
                confidence=0.82,
                evidence_refs=("p60",),
            ),
        ]
    )

    assert len(positions) == 2
    disclosed_positions = [row for row in positions if row.manager_name == "Alpha Capital"]
    assert disclosed_positions
    assert disclosed_positions[0].fund_name == "Fund I"
    assert not hasattr(disclosed_positions[0], "security_id")
    assert any(warning.code == "non_disclosure" for warning in warnings)


def test_allocation_and_fee_extraction_is_deterministic() -> None:
    fixture = _load_fixture()
    allocation_payload = fixture["allocation_table_and_amount_mix"]
    fee_payload = fixture["fee_schedule_partial_and_ambiguous"]

    def _allocations() -> list[AllocationDisclosureInput]:
        return [
            AllocationDisclosureInput(
                category_label=row["category_label"],
                percent_value=row["percent_value"],
                amount_value=row["amount_value"],
                amount_unit=row["amount_unit"],
                evidence_refs=tuple(row["evidence_refs"]),
            )
            for row in allocation_payload["allocation_rows"]
        ]

    def _fees() -> list[FeeDisclosureInput]:
        return [
            FeeDisclosureInput(
                manager_name=row["manager_name"],
                fee_type=row["fee_type"],
                rate_value=row["rate_value"],
                amount_value=row["amount_value"],
                amount_unit=row["amount_unit"],
                explicit_not_disclosed=row["explicit_not_disclosed"],
                evidence_refs=tuple(row["evidence_refs"]),
            )
            for row in fee_payload["fee_rows"]
        ]

    first = (
        extract_asset_allocations(
            plan_id=allocation_payload["plan_id"],
            plan_period=allocation_payload["plan_period"],
            effective_date=allocation_payload["effective_date"],
            ingestion_date=allocation_payload["ingestion_date"],
            source_document_id=allocation_payload["source_document_id"],
            rows=_allocations(),
        ),
        extract_fee_observations(
            plan_id=fee_payload["plan_id"],
            plan_period=fee_payload["plan_period"],
            effective_date=fee_payload["effective_date"],
            ingestion_date=fee_payload["ingestion_date"],
            source_document_id=fee_payload["source_document_id"],
            rows=_fees(),
        ),
    )
    second = (
        extract_asset_allocations(
            plan_id=allocation_payload["plan_id"],
            plan_period=allocation_payload["plan_period"],
            effective_date=allocation_payload["effective_date"],
            ingestion_date=allocation_payload["ingestion_date"],
            source_document_id=allocation_payload["source_document_id"],
            rows=list(reversed(_allocations())),
        ),
        extract_fee_observations(
            plan_id=fee_payload["plan_id"],
            plan_period=fee_payload["plan_period"],
            effective_date=fee_payload["effective_date"],
            ingestion_date=fee_payload["ingestion_date"],
            source_document_id=fee_payload["source_document_id"],
            rows=list(reversed(_fees())),
        ),
    )
    assert first == second
