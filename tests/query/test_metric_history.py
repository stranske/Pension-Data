"""Tests for metric-history query service and route adapter behavior."""

from __future__ import annotations

import pytest

from pension_data.api.auth import SCOPE_EXPORT, SCOPE_QUERY, APIKeyStore, ScopeDeniedError
from pension_data.api.routes.metric_history import run_metric_history_endpoint
from pension_data.db.models.core_facts import (
    ActuarialFact,
    BitemporalFactContext,
    DualReportedValue,
    FundedStatusFact,
)
from pension_data.query.metric_history_service import (
    MetricHistoryRequest,
    MetricHistoryRow,
    build_metric_history_rows,
    query_metric_history,
)


def _context(
    *,
    plan_id: str,
    plan_period: str,
    effective_date: str,
    ingestion_date: str,
    source_document_id: str,
) -> BitemporalFactContext:
    return BitemporalFactContext(
        plan_id=plan_id,
        plan_period=plan_period,
        effective_date=effective_date,
        ingestion_date=ingestion_date,
        benchmark_version="v1",
        source_document_id=source_document_id,
    )


def _value(
    *,
    as_reported_value: float | None,
    normalized_value: float | None,
    as_reported_unit: str | None,
    normalized_unit: str | None,
) -> DualReportedValue:
    return DualReportedValue(
        as_reported_value=as_reported_value,
        normalized_value=normalized_value,
        as_reported_unit=as_reported_unit,
        normalized_unit=normalized_unit,
    )


def _seed_history_rows() -> list[MetricHistoryRow]:
    funded_facts = (
        FundedStatusFact(
            context=_context(
                plan_id="CA-PERS",
                plan_period="FY2023",
                effective_date="2023-06-30",
                ingestion_date="2024-01-10",
                source_document_id="doc:ca:2023:funded",
            ),
            metric_name="funded_ratio",
            metric_value=_value(
                as_reported_value=74.0,
                normalized_value=0.74,
                as_reported_unit="percent",
                normalized_unit="ratio",
            ),
            confidence=0.94,
            evidence_refs=("p.12",),
        ),
        FundedStatusFact(
            context=_context(
                plan_id="CA-PERS",
                plan_period="FY2024",
                effective_date="2024-06-30",
                ingestion_date="2025-01-15",
                source_document_id="doc:ca:2024:funded",
            ),
            metric_name="funded_ratio",
            metric_value=_value(
                as_reported_value=79.5,
                normalized_value=0.795,
                as_reported_unit="percent",
                normalized_unit="ratio",
            ),
            confidence=0.95,
            evidence_refs=("p.45#funded ratio table",),
        ),
        FundedStatusFact(
            context=_context(
                plan_id="CA-PERS",
                plan_period="FY2024",
                effective_date="2024-06-30",
                ingestion_date="2025-03-01",
                source_document_id="doc:ca:2024:funded-revised",
            ),
            metric_name="funded_ratio",
            metric_value=_value(
                as_reported_value=80.1,
                normalized_value=0.801,
                as_reported_unit="percent",
                normalized_unit="ratio",
            ),
            confidence=0.97,
            evidence_refs=("p.46#restatement", "text:3"),
        ),
        FundedStatusFact(
            context=_context(
                plan_id="TX-ERS",
                plan_period="FY2024",
                effective_date="2024-08-31",
                ingestion_date="2025-02-01",
                source_document_id="doc:tx:2024:funded",
            ),
            metric_name="funded_ratio",
            metric_value=_value(
                as_reported_value=77.0,
                normalized_value=0.77,
                as_reported_unit="percent",
                normalized_unit="ratio",
            ),
            confidence=0.92,
            evidence_refs=("p.22",),
        ),
    )

    actuarial_facts = (
        ActuarialFact(
            context=_context(
                plan_id="CA-PERS",
                plan_period="FY2024",
                effective_date="2024-06-30",
                ingestion_date="2025-01-15",
                source_document_id="doc:ca:2024:actuarial",
            ),
            metric_name="normal_cost_rate",
            metric_value=_value(
                as_reported_value=11.2,
                normalized_value=0.112,
                as_reported_unit="percent",
                normalized_unit="ratio",
            ),
            confidence=0.91,
            evidence_refs=("p.51",),
        ),
    )
    return build_metric_history_rows(funded_facts=funded_facts, actuarial_facts=actuarial_facts)


def test_metric_history_returns_ordered_series_with_provenance_and_dual_values() -> None:
    rows = _seed_history_rows()
    response = query_metric_history(
        rows,
        request=MetricHistoryRequest(
            entity_id="CA-PERS",
            metric_name="funded_ratio",
        ),
    )

    assert response.total_rows == 3
    assert [(row.effective_date, row.ingestion_date) for row in response.rows] == [
        ("2023-06-30", "2024-01-10"),
        ("2024-06-30", "2025-01-15"),
        ("2024-06-30", "2025-03-01"),
    ]
    assert response.rows[1].as_reported_value == 79.5
    assert response.rows[1].normalized_value == 0.795
    assert response.rows[1].as_reported_unit == "percent"
    assert response.rows[1].normalized_unit == "ratio"
    assert response.rows[1].report_id == "doc:ca:2024:funded"
    assert response.rows[1].source_document_id == "doc:ca:2024:funded"
    assert any(ref.page_number == 45 for ref in response.rows[1].provenance_refs)


def test_metric_history_supports_bitemporal_window_filtering_for_revised_data() -> None:
    rows = _seed_history_rows()

    as_reported = query_metric_history(
        rows,
        request=MetricHistoryRequest(
            entity_id="CA-PERS",
            metric_name="funded_ratio",
            effective_start="2024-01-01",
            effective_end="2024-12-31",
            ingestion_end="2025-01-31",
        ),
    )
    assert as_reported.total_rows == 1
    assert as_reported.rows[0].source_document_id == "doc:ca:2024:funded"
    assert as_reported.rows[0].normalized_value == 0.795

    revised_window = query_metric_history(
        rows,
        request=MetricHistoryRequest(
            entity_id="CA-PERS",
            metric_name="funded_ratio",
            effective_start="2024-01-01",
            effective_end="2024-12-31",
            ingestion_end="2025-12-31",
        ),
    )
    assert revised_window.total_rows == 2
    assert [row.source_document_id for row in revised_window.rows] == [
        "doc:ca:2024:funded",
        "doc:ca:2024:funded-revised",
    ]


def test_metric_history_route_enforces_scope_and_emits_audit_context() -> None:
    rows = _seed_history_rows()
    store = APIKeyStore()
    unauthorized_secret, _ = store.create_key(scopes=(SCOPE_EXPORT,))
    with pytest.raises(ScopeDeniedError):
        run_metric_history_endpoint(
            api_key_header=unauthorized_secret,
            key_store=store,
            request=MetricHistoryRequest(entity_id="CA-PERS", metric_name="funded_ratio"),
            rows=rows,
        )

    authorized_secret, record = store.create_key(scopes=(SCOPE_QUERY,), label="analytics")
    result = run_metric_history_endpoint(
        api_key_header=authorized_secret,
        key_store=store,
        request=MetricHistoryRequest(
            entity_id="CA-PERS",
            metric_family="actuarial",
            limit=10,
        ),
        rows=rows,
        event={"request_id": "mh-001"},
    )
    assert result.response.total_rows == 1
    assert result.response.rows[0].metric_name == "normal_cost_rate"
    assert result.audit_event["operation"] == "query.metric_history"
    assert result.audit_event["api_key_id"] == record.key_id
    assert result.audit_event["request_id"] == "mh-001"
    assert result.audit_event["entity_id"] == "CA-PERS"
    assert result.audit_event["returned_rows"] == 1


def test_metric_history_rejects_invalid_temporal_filters() -> None:
    rows = _seed_history_rows()
    with pytest.raises(ValueError, match="effective_start must be an ISO-8601"):
        query_metric_history(
            rows,
            request=MetricHistoryRequest(
                entity_id="CA-PERS",
                effective_start="not-a-date",
            ),
        )


def test_metric_history_limit_applies_after_ordering() -> None:
    rows = _seed_history_rows()
    response = query_metric_history(
        rows,
        request=MetricHistoryRequest(
            entity_id="CA-PERS",
            metric_name="funded_ratio",
            limit=2,
        ),
    )
    assert response.total_rows == 3
    assert len(response.rows) == 2
    assert response.rows[0].effective_date == "2023-06-30"
    assert response.rows[1].ingestion_date == "2025-01-15"
