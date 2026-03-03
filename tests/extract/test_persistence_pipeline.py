"""Integration tests for funded/investment extraction persistence adapters."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from pension_data.extract.actuarial.metrics import (
    RawFundedActuarialInput,
    extract_funded_and_actuarial_metrics,
)
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
from pension_data.extract.persistence import (
    NON_DISCLOSED_MANAGER_NAME,
    PositionPersistenceContext,
    WarningPersistenceContext,
    build_extraction_persistence_artifacts,
    extraction_persistence_contract,
    persist_extraction_warnings,
    persist_funded_actuarial_metrics,
    write_extraction_persistence_artifacts,
)

FUNDED_FIXTURE_PATH = Path(__file__).parent / "funded" / "fixtures" / "funded_actuarial_golden.json"
INVESTMENT_FIXTURE_PATH = (
    Path(__file__).parent / "investment" / "fixtures" / "allocation_fee_golden.json"
)


def _load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def _raw_funded(payload: dict[str, Any]) -> RawFundedActuarialInput:
    return RawFundedActuarialInput(
        source_document_id=payload["source_document_id"],
        source_url=payload["source_url"],
        effective_date=payload["effective_date"],
        ingestion_date=payload["ingestion_date"],
        default_money_unit_scale=payload["default_money_unit_scale"],
        text_blocks=tuple(payload["text_blocks"]),
        table_rows=tuple(payload["table_rows"]),
    )


def test_persistence_contract_defines_target_tables_and_columns() -> None:
    contract = extraction_persistence_contract()

    assert set(contract) == {
        "staging_core_metrics",
        "staging_manager_fund_vehicle_relationships",
        "extraction_warnings",
    }
    assert "effective_date" in contract["staging_core_metrics"]
    assert "ingestion_date" in contract["staging_core_metrics"]
    assert "source_document_id" in contract["staging_core_metrics"]
    assert "evidence_refs" in contract["staging_core_metrics"]
    assert "relationship_completeness" in contract["staging_manager_fund_vehicle_relationships"]
    assert "known_not_invested" in contract["staging_manager_fund_vehicle_relationships"]
    assert "code" in contract["extraction_warnings"]
    assert "message" in contract["extraction_warnings"]


def test_extraction_to_persistence_pipeline_writes_core_rows_relationships_and_warnings(
    tmp_path: Path,
) -> None:
    funded_fixture = _load_json(FUNDED_FIXTURE_PATH)["text_layout_missing_participants"]
    funded_rows, funded_diagnostics = extract_funded_and_actuarial_metrics(
        plan_id=funded_fixture["plan_id"],
        plan_period=funded_fixture["plan_period"],
        raw=_raw_funded(funded_fixture["raw"]),
    )

    investment_fixture = _load_json(INVESTMENT_FIXTURE_PATH)
    allocation_payload = investment_fixture["allocation_table_and_amount_mix"]
    fee_payload = investment_fixture["fee_schedule_partial_and_ambiguous"]
    allocation_rows = extract_asset_allocations(
        plan_id=allocation_payload["plan_id"],
        plan_period=allocation_payload["plan_period"],
        effective_date=allocation_payload["effective_date"],
        ingestion_date=allocation_payload["ingestion_date"],
        source_document_id=allocation_payload["source_document_id"],
        rows=[
            AllocationDisclosureInput(
                category_label=row["category_label"],
                percent_value=row["percent_value"],
                amount_value=row["amount_value"],
                amount_unit=row["amount_unit"],
                evidence_refs=tuple(row["evidence_refs"]),
            )
            for row in allocation_payload["allocation_rows"]
        ],
    )
    fee_rows, investment_warnings = extract_fee_observations(
        plan_id=fee_payload["plan_id"],
        plan_period=fee_payload["plan_period"],
        effective_date=fee_payload["effective_date"],
        ingestion_date=fee_payload["ingestion_date"],
        source_document_id=fee_payload["source_document_id"],
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
            for row in fee_payload["fee_rows"]
        ],
    )

    manager_positions, manager_warnings = build_manager_fund_positions(
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

    artifacts = build_extraction_persistence_artifacts(
        funded_actuarial_rows=funded_rows,
        funded_actuarial_diagnostics=funded_diagnostics,
        funded_warning_context=WarningPersistenceContext(
            plan_id=funded_fixture["plan_id"],
            plan_period=funded_fixture["plan_period"],
            effective_date=funded_fixture["raw"]["effective_date"],
            ingestion_date=funded_fixture["raw"]["ingestion_date"],
            source_document_id=funded_fixture["raw"]["source_document_id"],
            source_url=funded_fixture["raw"]["source_url"],
        ),
        allocation_rows=allocation_rows,
        fee_rows=fee_rows,
        investment_warnings=investment_warnings,
        manager_position_rows=manager_positions,
        manager_position_warnings=manager_warnings,
        manager_position_context=PositionPersistenceContext(
            effective_date="2025-06-30",
            ingestion_date="2026-01-15",
            source_document_id="doc:ca:2025:manager_positions",
            source_url="https://example.gov/ca-2025-manager-disclosures.pdf",
            benchmark_version="v1",
        ),
        benchmark_version="v1",
    )

    core_rows = artifacts["staging_core_metrics_rows"]
    assert isinstance(core_rows, list)
    assert any(
        row["metric_family"] == "funded"
        and row["metric_name"] == "funded_ratio"
        and row["effective_date"] == "2024-08-31"
        and row["ingestion_date"] == "2025-01-20"
        and row["source_document_id"] == "doc:tx:2024:funded"
        for row in core_rows
    )
    assert any(
        row["metric_family"] == "allocation"
        and row["metric_name"] == "public_equity_weight"
        and row["as_reported_value"] == 42.5
        and row["normalized_value"] == 0.425
        for row in core_rows
    )
    assert any(
        row["metric_family"] == "fee"
        and row["metric_name"] == "management_fee_rate"
        and row["manager_name"] == "Alpha Capital"
        and row["normalized_value"] == 0.015
        for row in core_rows
    )
    assert any(
        row["metric_family"] == "holding"
        and row["metric_name"] == "market_value"
        and row["manager_name"] == "Alpha Capital"
        and row["fund_name"] == "Fund I"
        for row in core_rows
    )

    relationship_rows = artifacts["staging_manager_fund_vehicle_relationship_rows"]
    assert isinstance(relationship_rows, list)
    assert any(
        row["manager_name"] == NON_DISCLOSED_MANAGER_NAME
        and row["relationship_completeness"] == "not_disclosed"
        for row in relationship_rows
    )

    warning_rows = artifacts["extraction_warning_rows"]
    assert isinstance(warning_rows, list)
    warning_codes = {row["code"] for row in warning_rows}
    assert "missing_metric" in warning_codes
    assert "non_disclosure" in warning_codes

    output_paths = write_extraction_persistence_artifacts(artifacts, output_root=tmp_path)
    saved_core_rows = json.loads(Path(output_paths["staging_core_metrics_json"]).read_text())
    saved_relationship_rows = json.loads(
        Path(output_paths["staging_manager_fund_vehicle_relationships_json"]).read_text()
    )
    saved_warning_rows = json.loads(Path(output_paths["extraction_warnings_json"]).read_text())
    assert saved_core_rows == core_rows
    assert saved_relationship_rows == relationship_rows
    assert saved_warning_rows == warning_rows


def test_persistence_artifacts_are_deterministic_for_input_order() -> None:
    funded_fixture = _load_json(FUNDED_FIXTURE_PATH)["table_layout_complete"]
    funded_rows, funded_diagnostics = extract_funded_and_actuarial_metrics(
        plan_id=funded_fixture["plan_id"],
        plan_period=funded_fixture["plan_period"],
        raw=_raw_funded(funded_fixture["raw"]),
    )
    investment_fixture = _load_json(INVESTMENT_FIXTURE_PATH)
    allocation_payload = investment_fixture["allocation_table_and_amount_mix"]
    fee_payload = investment_fixture["fee_schedule_partial_and_ambiguous"]

    allocation_inputs = [
        AllocationDisclosureInput(
            category_label=row["category_label"],
            percent_value=row["percent_value"],
            amount_value=row["amount_value"],
            amount_unit=row["amount_unit"],
            evidence_refs=tuple(row["evidence_refs"]),
        )
        for row in allocation_payload["allocation_rows"]
    ]
    fee_inputs = [
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
    allocation_rows = extract_asset_allocations(
        plan_id=allocation_payload["plan_id"],
        plan_period=allocation_payload["plan_period"],
        effective_date=allocation_payload["effective_date"],
        ingestion_date=allocation_payload["ingestion_date"],
        source_document_id=allocation_payload["source_document_id"],
        rows=allocation_inputs,
    )
    fee_rows, investment_warnings = extract_fee_observations(
        plan_id=fee_payload["plan_id"],
        plan_period=fee_payload["plan_period"],
        effective_date=fee_payload["effective_date"],
        ingestion_date=fee_payload["ingestion_date"],
        source_document_id=fee_payload["source_document_id"],
        rows=fee_inputs,
    )
    manager_positions, manager_warnings = build_manager_fund_positions(
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
    context = PositionPersistenceContext(
        effective_date="2025-06-30",
        ingestion_date="2026-01-15",
        source_document_id="doc:ca:2025:manager_positions",
        source_url="https://example.gov/ca-2025-manager-disclosures.pdf",
        benchmark_version="v1",
    )
    funded_warning_context = WarningPersistenceContext(
        plan_id=funded_fixture["plan_id"],
        plan_period=funded_fixture["plan_period"],
        effective_date=funded_fixture["raw"]["effective_date"],
        ingestion_date=funded_fixture["raw"]["ingestion_date"],
        source_document_id=funded_fixture["raw"]["source_document_id"],
        source_url=funded_fixture["raw"]["source_url"],
    )

    first = build_extraction_persistence_artifacts(
        funded_actuarial_rows=funded_rows,
        funded_actuarial_diagnostics=funded_diagnostics,
        funded_warning_context=funded_warning_context,
        allocation_rows=allocation_rows,
        fee_rows=fee_rows,
        investment_warnings=investment_warnings,
        manager_position_rows=manager_positions,
        manager_position_warnings=manager_warnings,
        manager_position_context=context,
        benchmark_version="v1",
    )
    second = build_extraction_persistence_artifacts(
        funded_actuarial_rows=list(reversed(funded_rows)),
        funded_actuarial_diagnostics=list(reversed(funded_diagnostics)),
        funded_warning_context=funded_warning_context,
        allocation_rows=list(reversed(allocation_rows)),
        fee_rows=list(reversed(fee_rows)),
        investment_warnings=list(reversed(investment_warnings)),
        manager_position_rows=list(reversed(manager_positions)),
        manager_position_warnings=list(reversed(manager_warnings)),
        manager_position_context=context,
        benchmark_version="v1",
    )

    assert first == second


def test_persisted_fact_ids_use_normalized_evidence_refs() -> None:
    funded_fixture = _load_json(FUNDED_FIXTURE_PATH)["table_layout_complete"]
    funded_rows, _ = extract_funded_and_actuarial_metrics(
        plan_id=funded_fixture["plan_id"],
        plan_period=funded_fixture["plan_period"],
        raw=_raw_funded(funded_fixture["raw"]),
    )
    row = funded_rows[0]

    id_from_whitespace_dupes = persist_funded_actuarial_metrics(
        [replace(row, evidence_refs=(" p14 ", "p14"))],
        benchmark_version="v1",
    )[0]
    id_from_canonical_refs = persist_funded_actuarial_metrics(
        [replace(row, evidence_refs=("p14",))],
        benchmark_version="v1",
    )[0]

    assert id_from_whitespace_dupes["fact_id"] == id_from_canonical_refs["fact_id"]
    assert id_from_whitespace_dupes["evidence_refs"] == ["p14"]


def test_funded_diagnostics_require_warning_context() -> None:
    funded_fixture = _load_json(FUNDED_FIXTURE_PATH)["text_layout_missing_participants"]
    _, funded_diagnostics = extract_funded_and_actuarial_metrics(
        plan_id=funded_fixture["plan_id"],
        plan_period=funded_fixture["plan_period"],
        raw=_raw_funded(funded_fixture["raw"]),
    )

    with pytest.raises(ValueError, match="funded_context is required"):
        build_extraction_persistence_artifacts(
            funded_actuarial_diagnostics=funded_diagnostics,
        )


def test_investment_warnings_use_null_when_temporal_provenance_is_unknown() -> None:
    investment_fixture = _load_json(INVESTMENT_FIXTURE_PATH)
    fee_payload = investment_fixture["fee_schedule_partial_and_ambiguous"]
    _, investment_warnings = extract_fee_observations(
        plan_id=fee_payload["plan_id"],
        plan_period=fee_payload["plan_period"],
        effective_date=fee_payload["effective_date"],
        ingestion_date=fee_payload["ingestion_date"],
        source_document_id=fee_payload["source_document_id"],
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
            for row in fee_payload["fee_rows"]
        ],
    )

    warning_rows = persist_extraction_warnings(investment_warnings=investment_warnings)
    for row in warning_rows:
        assert row["effective_date"] is None
        assert row["ingestion_date"] is None
        assert row["source_document_id"] is None
        assert row["source_url"] is None
