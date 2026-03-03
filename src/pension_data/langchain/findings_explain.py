"""Findings explanation chain with deterministic schema and citation controls."""

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

ExplainStatus = Literal["ok", "error"]


@dataclass(frozen=True, slots=True)
class ExplainRequest:
    """Input contract for one explain request."""

    question: str
    finding_slice: FindingSlice


@dataclass(frozen=True, slots=True)
class ExplainResult:
    """Structured explain result schema."""

    summary: str
    key_drivers: tuple[str, ...]
    caveats: tuple[str, ...]
    citations: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ExplainMetadata:
    """Deterministic metadata for explain responses."""

    request_id: str
    generated_at: str
    chain_name: str


@dataclass(frozen=True, slots=True)
class ExplainResponse:
    """Explain response envelope with status and schema-guaranteed payload."""

    status: ExplainStatus
    result: ExplainResult | None
    metadata: ExplainMetadata
    error: str | None


class FindingsExplainChain(Protocol):
    """Minimal protocol for explain-style chain adapters."""

    def invoke(self, values: Mapping[str, Any]) -> Mapping[str, Any] | str:
        """Invoke explain chain with structured inputs."""


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


def run_findings_explain_chain(
    *,
    request: ExplainRequest,
    chain: FindingsExplainChain,
    request_id: str | None = None,
) -> ExplainResponse:
    """Execute explain chain and normalize output to deterministic schema."""
    trace_id = request_id or f"fx:{uuid4().hex}"
    metadata = ExplainMetadata(
        request_id=trace_id,
        generated_at=_utc_now_iso(),
        chain_name="findings_explain",
    )
    try:
        question = ensure_question(request.question)
        chain_output = chain.invoke(
            {
                "question": question,
                "slice": _slice_payload(request.finding_slice),
                "schema_contract": {
                    "summary": "string",
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
        key_drivers = normalize_string_tuple(payload.get("key_drivers"))
        caveats = normalize_string_tuple(payload.get("caveats"))
        citations = filter_allowed_citations(
            proposed=normalize_string_tuple(payload.get("citations")),
            allowed=request.finding_slice.citations,
        )
        return ExplainResponse(
            status="ok",
            result=ExplainResult(
                summary=summary,
                key_drivers=key_drivers,
                caveats=caveats,
                citations=citations,
            ),
            metadata=metadata,
            error=None,
        )
    except Exception as exc:  # noqa: BLE001
        return ExplainResponse(
            status="error",
            result=None,
            metadata=metadata,
            error=normalize_text(str(exc)) or "explain chain execution failed",
        )
