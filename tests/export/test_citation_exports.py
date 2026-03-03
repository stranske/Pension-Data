"""Tests for citation-ready export schemas and completeness behavior."""

from __future__ import annotations

import pytest

from pension_data.api.auth import SCOPE_EXPORT, SCOPE_QUERY, APIKeyStore, ScopeDeniedError
from pension_data.api.routes.export import (
    run_metric_history_export_endpoint,
    run_sql_export_endpoint,
)
from pension_data.export import (
    CITATION_COLUMNS,
    METRIC_HISTORY_BASE_COLUMNS,
    SCHEMA_VERSION,
    SQL_BASE_COLUMNS,
    CitationReference,
    MetricHistoryExportInput,
    build_metric_history_citation_export,
    build_sql_citation_export,
)


def test_sql_export_schema_is_stable_and_missing_provenance_is_explicit() -> None:
    export_payload = build_sql_citation_export(
        columns=("metric_name", "value"),
        rows=(
            ("funded_ratio", 0.79),
            ("discount_rate", 0.0675),
        ),
        provenance_by_row_id={
            "sql:1": (
                CitationReference(
                    report_id="doc:ca:2024",
                    source_document_id="doc:ca:2024",
                    source_url="https://example.org/ca/2024.pdf",
                    page_number=45,
                    evidence_ref="p.45",
                ),
                CitationReference(
                    report_id="doc:ca:2024",
                    source_document_id="doc:ca:2024",
                    source_url="https://example.org/ca/2024.pdf",
                    page_number=47,
                    evidence_ref="p.47",
                ),
            )
        },
    )

    assert export_payload.schema_name == "sql_citation_export"
    assert export_payload.schema_version == SCHEMA_VERSION
    assert export_payload.field_names == (
        *SQL_BASE_COLUMNS,
        "metric_name",
        "value",
        *CITATION_COLUMNS,
    )
    assert len(export_payload.rows) == 3
    assert export_payload.rows[0]["row_id"] == "sql:1"
    assert export_payload.rows[0]["citation_status"] == "present"
    assert export_payload.rows[0]["citation_page_number"] == 45
    assert export_payload.rows[1]["citation_page_number"] == 47
    assert export_payload.rows[2]["row_id"] == "sql:2"
    assert export_payload.rows[2]["citation_status"] == "missing"
    assert export_payload.rows[2]["citation_source_url"] is None

    assert len(export_payload.citation_bundle.citations) == 2
    assert export_payload.citation_bundle.missing_row_ids == ("sql:2",)


def test_sql_export_rejects_reserved_column_name_collisions() -> None:
    with pytest.raises(ValueError, match="reserved export field names"):
        build_sql_citation_export(
            columns=("row_id", "metric_name"),
            rows=(("custom-id", "funded_ratio"),),
        )


def test_metric_history_export_is_deterministic_and_surfaces_missing_provenance() -> None:
    export_payload = build_metric_history_citation_export(
        (
            MetricHistoryExportInput(
                entity_id="CA-PERS",
                plan_period="FY2024",
                metric_family="funded",
                metric_name="funded_ratio",
                as_reported_value=79.5,
                normalized_value=0.795,
                as_reported_unit="percent",
                normalized_unit="ratio",
                confidence=0.95,
                effective_date="2024-06-30",
                ingestion_date="2025-01-15",
                benchmark_version="v1",
                report_id="doc:ca:2024",
                source_document_id="doc:ca:2024",
                provenance_refs=(
                    CitationReference(
                        report_id="doc:ca:2024",
                        source_document_id="doc:ca:2024",
                        source_url="https://example.org/ca/2024.pdf",
                        page_number=45,
                        evidence_ref="p.45",
                    ),
                ),
            ),
            MetricHistoryExportInput(
                entity_id="CA-PERS",
                plan_period="FY2025",
                metric_family="funded",
                metric_name="funded_ratio",
                as_reported_value=80.1,
                normalized_value=0.801,
                as_reported_unit="percent",
                normalized_unit="ratio",
                confidence=0.97,
                effective_date="2025-06-30",
                ingestion_date="2026-01-15",
                benchmark_version="v1",
                report_id="doc:ca:2025",
                source_document_id="doc:ca:2025",
                provenance_refs=(),
            ),
        )
    )

    assert export_payload.schema_name == "metric_history_citation_export"
    assert export_payload.schema_version == SCHEMA_VERSION
    assert export_payload.field_names == (*METRIC_HISTORY_BASE_COLUMNS, *CITATION_COLUMNS)
    assert len(export_payload.rows) == 2
    assert [row["row_id"] for row in export_payload.rows] == [
        "metric-history:1",
        "metric-history:2",
    ]
    assert export_payload.rows[0]["citation_status"] == "present"
    assert export_payload.rows[1]["citation_status"] == "missing"
    assert export_payload.citation_bundle.missing_row_ids == ("metric-history:2",)


def test_metric_history_export_order_is_stable_for_tie_cases() -> None:
    rows = (
        MetricHistoryExportInput(
            entity_id="CA-PERS",
            plan_period="FY2025",
            metric_family="funded",
            metric_name="funded_ratio",
            as_reported_value=80.1,
            normalized_value=0.801,
            as_reported_unit="percent",
            normalized_unit="ratio",
            confidence=0.97,
            effective_date="2025-06-30",
            ingestion_date="2026-01-15",
            benchmark_version="v1",
            report_id="doc:ca:2025:b",
            source_document_id="doc:ca:2025",
            provenance_refs=(),
        ),
        MetricHistoryExportInput(
            entity_id="CA-PERS",
            plan_period="FY2024",
            metric_family="funded",
            metric_name="funded_ratio",
            as_reported_value=79.5,
            normalized_value=0.795,
            as_reported_unit="percent",
            normalized_unit="ratio",
            confidence=0.95,
            effective_date="2025-06-30",
            ingestion_date="2026-01-15",
            benchmark_version="v1",
            report_id="doc:ca:2025:a",
            source_document_id="doc:ca:2025",
            provenance_refs=(),
        ),
    )
    export_payload = build_metric_history_citation_export(rows)
    assert [item["plan_period"] for item in export_payload.rows] == ["FY2024", "FY2025"]
    assert [item["report_id"] for item in export_payload.rows] == [
        "doc:ca:2025:a",
        "doc:ca:2025:b",
    ]


def test_export_route_requires_export_scope() -> None:
    key_store = APIKeyStore()
    unauthorized_secret, _ = key_store.create_key(scopes=(SCOPE_QUERY,))
    with pytest.raises(ScopeDeniedError):
        run_sql_export_endpoint(
            api_key_header=unauthorized_secret,
            key_store=key_store,
            columns=("metric_name",),
            rows=(("funded_ratio",),),
        )


def test_export_route_emits_audit_context_for_metric_history_exports() -> None:
    key_store = APIKeyStore()
    secret, record = key_store.create_key(scopes=(SCOPE_EXPORT,), label="export-client")
    row = MetricHistoryExportInput(
        entity_id="CA-PERS",
        plan_period="FY2024",
        metric_family="funded",
        metric_name="funded_ratio",
        as_reported_value=79.5,
        normalized_value=0.795,
        as_reported_unit="percent",
        normalized_unit="ratio",
        confidence=0.95,
        effective_date="2024-06-30",
        ingestion_date="2025-01-15",
        benchmark_version="v1",
        report_id="doc:ca:2024",
        source_document_id="doc:ca:2024",
        provenance_refs=(),
    )
    result = run_metric_history_export_endpoint(
        api_key_header=secret,
        key_store=key_store,
        rows=(row,),
        event={"request_id": "exp-001", "operation": "caller-overwrite"},
    )

    assert result.export.schema_name == "metric_history_citation_export"
    assert result.audit_event["operation"] == "export.metric_history"
    assert result.audit_event["api_key_id"] == record.key_id
    assert result.audit_event["request_id"] == "exp-001"
    assert result.audit_event["row_count"] == 1
    assert result.audit_event["missing_provenance_rows"] == 1
