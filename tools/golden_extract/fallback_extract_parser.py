"""Golden-corpus parser callable that exercises extraction fallback orchestration."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from pension_data.db.models.funded_actuarial import FUNDED_ACTUARIAL_REQUIRED_METRICS
from pension_data.extract.actuarial.metrics import (
    RawFundedActuarialInput,
    extract_funded_and_actuarial_metrics,
)
from pension_data.extract.orchestration.fallback import (
    PARSER_FALLBACK_ORDER_BY_DOMAIN,
    ParserStage,
    run_fallback_chain,
)
from pension_data.normalize.financial_units import UnitScale
from tools.replay.harness import CorpusDocument, FieldExtraction


@dataclass(frozen=True, slots=True)
class _FundedStageResult:
    fields: dict[str, FieldExtraction]
    missing_metrics: tuple[str, ...]


def _parse_payload(document: CorpusDocument) -> dict[str, object]:
    payload = json.loads(document.content)
    if not isinstance(payload, dict):
        raise ValueError("document.content must decode to a JSON object")
    return payload


def _require_string(payload: Mapping[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"payload missing required string field '{key}'")
    return value


def _as_raw_funded(payload: Mapping[str, object]) -> RawFundedActuarialInput:
    raw = payload.get("raw")
    if not isinstance(raw, Mapping):
        raise ValueError("payload.raw must be an object")
    text_blocks = raw.get("text_blocks")
    table_rows = raw.get("table_rows")
    if not isinstance(text_blocks, list) or not all(isinstance(item, str) for item in text_blocks):
        raise ValueError("payload.raw.text_blocks must be a list[str]")
    if not isinstance(table_rows, list) or not all(
        isinstance(item, Mapping) for item in table_rows
    ):
        raise ValueError("payload.raw.table_rows must be a list[object]")
    return RawFundedActuarialInput(
        source_document_id=_require_string(raw, "source_document_id"),
        source_url=_require_string(raw, "source_url"),
        effective_date=_require_string(raw, "effective_date"),
        ingestion_date=_require_string(raw, "ingestion_date"),
        default_money_unit_scale=cast(UnitScale, _require_string(raw, "default_money_unit_scale")),
        text_blocks=tuple(text_blocks),
        table_rows=tuple(dict(item) for item in table_rows),
    )


def _run_funded_stage(
    *,
    plan_id: str,
    plan_period: str,
    raw: RawFundedActuarialInput,
    use_text_blocks: bool,
    use_table_rows: bool,
) -> _FundedStageResult:
    stage_raw = RawFundedActuarialInput(
        source_document_id=raw.source_document_id,
        source_url=raw.source_url,
        effective_date=raw.effective_date,
        ingestion_date=raw.ingestion_date,
        default_money_unit_scale=raw.default_money_unit_scale,
        text_blocks=raw.text_blocks if use_text_blocks else (),
        table_rows=raw.table_rows if use_table_rows else (),
    )
    facts, _ = extract_funded_and_actuarial_metrics(
        plan_id=plan_id,
        plan_period=plan_period,
        raw=stage_raw,
    )
    fields = {
        str(fact.metric_name): FieldExtraction(
            value=fact.normalized_value,
            confidence=fact.confidence,
            evidence=fact.evidence_refs[0] if fact.evidence_refs else None,
        )
        for fact in facts
    }
    missing_metrics = tuple(
        metric_name
        for metric_name in FUNDED_ACTUARIAL_REQUIRED_METRICS
        if metric_name not in fields
    )
    return _FundedStageResult(
        fields=fields,
        missing_metrics=missing_metrics,
    )


def parse(document: CorpusDocument) -> Mapping[str, FieldExtraction]:
    """Replay parser callable used by golden-corpus extraction checks."""
    payload = _parse_payload(document)
    domain = _require_string(payload, "domain")
    if domain != "funded":
        raise ValueError(f"unsupported domain '{domain}' in golden corpus")

    plan_id = _require_string(payload, "plan_id")
    plan_period = _require_string(payload, "plan_period")
    raw = _as_raw_funded(payload)
    order = PARSER_FALLBACK_ORDER_BY_DOMAIN["funded"]
    stage_builders: dict[str, ParserStage[_FundedStageResult]] = {
        "table_primary": ParserStage(
            stage_name="table_primary",
            parser_name="funded_table_only",
            parse=lambda: _run_funded_stage(
                plan_id=plan_id,
                plan_period=plan_period,
                raw=raw,
                use_text_blocks=False,
                use_table_rows=True,
            ),
        ),
        "text_fallback": ParserStage(
            stage_name="text_fallback",
            parser_name="funded_text_only",
            parse=lambda: _run_funded_stage(
                plan_id=plan_id,
                plan_period=plan_period,
                raw=raw,
                use_text_blocks=True,
                use_table_rows=False,
            ),
        ),
        "full_fallback": ParserStage(
            stage_name="full_fallback",
            parser_name="funded_full",
            parse=lambda: _run_funded_stage(
                plan_id=plan_id,
                plan_period=plan_period,
                raw=raw,
                use_text_blocks=True,
                use_table_rows=True,
            ),
        ),
    }

    outcome = run_fallback_chain(
        domain=domain,
        stages=[stage_builders[name] for name in order],
        is_complete=lambda result: not result.missing_metrics,
    )
    if outcome.result is None or outcome.escalation is not None:
        return {
            "__escalation__": FieldExtraction(
                value=outcome.escalation.reason if outcome.escalation is not None else "unknown",
                confidence=0.0,
                evidence=f"stages:{len(outcome.attempts)}",
            )
        }

    selected_stage = next(
        attempt.stage_name for attempt in reversed(outcome.attempts) if attempt.succeeded
    )
    with_stage = dict(outcome.result.fields)
    with_stage["__fallback_stage__"] = FieldExtraction(
        value=selected_stage,
        confidence=1.0,
        evidence=next(
            attempt.parser_name for attempt in reversed(outcome.attempts) if attempt.succeeded
        ),
    )
    return with_stage
