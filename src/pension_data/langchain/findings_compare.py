"""Findings comparison chain with deterministic schema and citation controls."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Protocol
from uuid import uuid4

from pension_data.langchain.findings_common import (
    FindingSlice,
    ensure_question,
    filter_allowed_citations,
    normalize_string_tuple,
    normalize_text,
)

CompareStatus = Literal["ok", "error"]


@dataclass(frozen=True, slots=True)
class CompareRequest:
    """Input contract for one compare request."""

    question: str
    left_slice: FindingSlice
    right_slice: FindingSlice


@dataclass(frozen=True, slots=True)
class CompareResult:
    """Structured compare result schema."""

    summary: str
    key_differences: tuple[str, ...]
    key_drivers: tuple[str, ...]
    caveats: tuple[str, ...]
    citations: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CompareMetadata:
    """Deterministic metadata for compare responses."""

    request_id: str
    generated_at: str
    chain_name: str


@dataclass(frozen=True, slots=True)
class CompareResponse:
    """Compare response envelope with status and schema-guaranteed payload."""

    status: CompareStatus
    result: CompareResult | None
    metadata: CompareMetadata
    error: str | None


class FindingsCompareChain(Protocol):
    """Minimal protocol for compare-style chain adapters."""

    def invoke(self, values: Mapping[str, Any]) -> Mapping[str, Any] | str:
        """Invoke compare chain with structured inputs."""


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _parse_chain_output(output: Mapping[str, Any] | str) -> Mapping[str, Any]:
    if isinstance(output, Mapping):
        return output
    if not isinstance(output, str):
        raise ValueError("chain output must be a mapping or JSON string")
    try:
        parsed = json.loads(output)
    except ValueError as exc:
        raise ValueError("chain output must be valid JSON") from exc
    if not isinstance(parsed, Mapping):
        raise ValueError("chain output JSON must decode to an object")
    return parsed


def _slice_payload(finding_slice: FindingSlice) -> dict[str, object]:
    return {
        "slice_id": finding_slice.slice_id,
        "plan_id": finding_slice.plan_id,
        "plan_period": finding_slice.plan_period,
        "metrics": dict(sorted(finding_slice.metrics.items())),
        "citations": list(finding_slice.citations),
    }


def run_findings_compare_chain(
    *,
    request: CompareRequest,
    chain: FindingsCompareChain,
    request_id: str | None = None,
) -> CompareResponse:
    """Execute compare chain and normalize output to deterministic schema."""
    trace_id = request_id or f"fc:{uuid4().hex}"
    metadata = CompareMetadata(
        request_id=trace_id,
        generated_at=_utc_now_iso(),
        chain_name="findings_compare",
    )
    try:
        question = ensure_question(request.question)
        chain_output = chain.invoke(
            {
                "question": question,
                "left_slice": _slice_payload(request.left_slice),
                "right_slice": _slice_payload(request.right_slice),
                "schema_contract": {
                    "summary": "string",
                    "key_differences": "string[]",
                    "key_drivers": "string[]",
                    "caveats": "string[]",
                    "citations": "string[]",
                },
            }
        )
        payload = _parse_chain_output(chain_output)
        summary = normalize_text(payload.get("summary"))
        if not summary:
            raise ValueError("summary is required")
        key_differences = normalize_string_tuple(payload.get("key_differences"))
        key_drivers = normalize_string_tuple(payload.get("key_drivers"))
        caveats = normalize_string_tuple(payload.get("caveats"))
        citations = filter_allowed_citations(
            proposed=normalize_string_tuple(payload.get("citations")),
            allowed=(*request.left_slice.citations, *request.right_slice.citations),
        )
        return CompareResponse(
            status="ok",
            result=CompareResult(
                summary=summary,
                key_differences=key_differences,
                key_drivers=key_drivers,
                caveats=caveats,
                citations=citations,
            ),
            metadata=metadata,
            error=None,
        )
    except Exception as exc:  # noqa: BLE001
        return CompareResponse(
            status="error",
            result=None,
            metadata=metadata,
            error=normalize_text(str(exc)) or "compare chain execution failed",
        )
