"""API route adapters for query/export-facing services."""

from pension_data.api.routes.export import (
    ExportRouteResult,
    run_metric_history_export_endpoint,
    run_sql_export_endpoint,
)
from pension_data.api.routes.findings import (
    FindingsCompareRouteResult,
    FindingsExplainRouteResult,
    run_findings_compare_endpoint,
    run_findings_explain_endpoint,
)
from pension_data.api.routes.metric_history import (
    MetricHistoryRouteResult,
    run_metric_history_endpoint,
)
from pension_data.api.routes.nl import NLRouteResult, run_nl_query_endpoint
from pension_data.api.routes.sql import run_sql_query_endpoint

__all__ = [
    "ExportRouteResult",
    "FindingsCompareRouteResult",
    "FindingsExplainRouteResult",
    "MetricHistoryRouteResult",
    "NLRouteResult",
    "run_findings_compare_endpoint",
    "run_findings_explain_endpoint",
    "run_metric_history_endpoint",
    "run_metric_history_export_endpoint",
    "run_nl_query_endpoint",
    "run_sql_export_endpoint",
    "run_sql_query_endpoint",
]
