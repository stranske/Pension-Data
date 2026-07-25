"""Public Plans Database (PPD) peer-universe ingest.

Client + on-disk cache + codebook parse + mapping onto the existing
``query/saved_views`` input dataclasses + peer-group derivation + a
provenance-bearing, idempotent peer-universe dataset. See issue #645.
"""

from pension_data.sources.ppd.cache import PpdResponseCache
from pension_data.sources.ppd.client import (
    PPD_API_BASE_URL,
    PpdApiError,
    PpdClient,
    build_codebook_url,
    build_qvariables_url,
    parse_qvariables_json,
)
from pension_data.sources.ppd.codebook import CodebookEntry, parse_codebook_csv
from pension_data.sources.ppd.mapping import (
    derive_maturity,
    derive_peer_group,
    derive_peer_group_components,
    derive_plan_type,
    derive_size,
    derive_state,
    to_benchmark_panel_input,
    to_funding_trend_input,
)
from pension_data.sources.ppd.mapping import (
    to_allocation_peer_inputs as record_to_allocation_peer_inputs,
)
from pension_data.sources.ppd.peer_universe import (
    PPD_SOURCE_NAME,
    PeerUniverseDataset,
    PeerUniverseRow,
    PeerUniverseStore,
    PpdProvenance,
    build_citation_payload,
    build_row_evidence_references,
    ingest_records,
    to_allocation_peer_inputs,
    to_benchmark_panel_inputs,
    to_funding_trend_inputs,
)
from pension_data.sources.ppd.variables import (
    PPD_VARIABLES,
    guessed_variable_names,
    request_variable_names,
    variable_name,
)

__all__ = [
    "PPD_API_BASE_URL",
    "PPD_SOURCE_NAME",
    "PPD_VARIABLES",
    "CodebookEntry",
    "PeerUniverseDataset",
    "PeerUniverseRow",
    "PeerUniverseStore",
    "PpdApiError",
    "PpdClient",
    "PpdProvenance",
    "PpdResponseCache",
    "build_citation_payload",
    "build_codebook_url",
    "build_qvariables_url",
    "build_row_evidence_references",
    "derive_maturity",
    "derive_peer_group",
    "derive_peer_group_components",
    "derive_plan_type",
    "derive_size",
    "derive_state",
    "guessed_variable_names",
    "ingest_records",
    "parse_codebook_csv",
    "parse_qvariables_json",
    "record_to_allocation_peer_inputs",
    "request_variable_names",
    "to_allocation_peer_inputs",
    "to_benchmark_panel_input",
    "to_benchmark_panel_inputs",
    "to_funding_trend_input",
    "to_funding_trend_inputs",
    "variable_name",
]
