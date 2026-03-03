"""Deterministic PDF parser pipeline with text/table/OCR fallback stages."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from io import BytesIO

from pypdf import PdfReader

from pension_data.db.models.funded_actuarial import (
    FUNDED_ACTUARIAL_REQUIRED_METRICS,
    FundedActuarialMetricName,
)
from pension_data.extract.actuarial.metrics import (
    RawFundedActuarialInput,
    extract_funded_and_actuarial_metrics,
)
from pension_data.extract.orchestration.fallback import (
    EscalationEvent,
    ParserAttempt,
    ParserStage,
    run_fallback_chain,
)
from pension_data.normalize.financial_units import UnitScale

_TEXT_TOKEN_PATTERN = re.compile(r"\((?P<token>(?:\\.|[^\\)])+)\)\s*Tj")
_TEXT_ARRAY_PATTERN = re.compile(r"\[(?P<array>(?:\\.|[^\]])*)\]\s*TJ", re.DOTALL)
_STRING_TOKEN_PATTERN = re.compile(r"\((?P<token>(?:\\.|[^\\)])+)\)")
_PAGE_MARKER_PATTERN = re.compile(r"(?m)^\s*%%Page:\s*\d+\s+\d+\s*$")
_TABLE_SPLIT_PATTERN = re.compile(r"\s{2,}|\|")
_NON_PRINTABLE_PATTERN = re.compile(r"[^\x20-\x7E]")
_VALUE_TOKEN_PATTERN = re.compile(r"[-+]?\$?\d[\d,]*(?:\.\d+)?%?")
_PDF_INTERNAL_TOKEN_PATTERN = re.compile(
    r"(?:\bobj\b|endobj|stream|endstream|xref|trailer|startxref|/type|/length|/filter|flatedecode)",
    re.IGNORECASE,
)
_METRIC_HINTS: tuple[str, ...] = (
    "funded ratio",
    "funding ratio",
    "aal",
    "actuarial accrued liability",
    "ava",
    "actuarial value of assets",
    "discount rate",
    "assumed return",
    "employer contribution rate",
    "adc rate",
    "employee contribution rate",
    "participant count",
    "active participants",
)


@dataclass(frozen=True, slots=True)
class PDFParserInput:
    """Input contract for parsing one pension PDF artifact."""

    source_document_id: str
    source_url: str
    effective_date: str
    ingestion_date: str
    default_money_unit_scale: UnitScale
    pdf_bytes: bytes
    ocr_extract: Callable[[bytes], Sequence[str]] | None = None


@dataclass(frozen=True, slots=True)
class ParserStageOutput:
    """Deterministic parser stage output contract."""

    text_blocks: tuple[str, ...]
    text_block_evidence_refs: tuple[str, ...]
    table_rows: tuple[dict[str, str], ...]
    stage_confidence: float


@dataclass(frozen=True, slots=True)
class PDFParserResult:
    """End-to-end parser result for funded/actuarial extraction readiness."""

    raw: RawFundedActuarialInput | None
    stage_name: str | None
    stage_confidence: float
    attempts: tuple[ParserAttempt, ...]
    escalation: EscalationEvent | None
    escalation_required: bool
    missing_metrics: tuple[FundedActuarialMetricName, ...]
    actionable_flags: tuple[str, ...]
    provenance_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _StageCandidate:
    stage_name: str
    output: ParserStageOutput
    raw: RawFundedActuarialInput
    missing_metrics: tuple[FundedActuarialMetricName, ...]


def _dedupe_non_empty(values: Sequence[str]) -> tuple[str, ...]:
    deduped: list[str] = []
    for value in values:
        token = value.strip()
        if not token or token in deduped:
            continue
        deduped.append(token)
    return tuple(deduped)


def _coerce_printable(text: str) -> str:
    collapsed = _NON_PRINTABLE_PATTERN.sub(" ", text)
    return " ".join(collapsed.split())


def _looks_like_noise_line(line: str) -> bool:
    lowered = line.lower().strip()
    if not lowered:
        return True
    if lowered.startswith("%pdf"):
        return True
    if lowered in {"xref", "trailer", "startxref", "stream", "endstream", "endobj", "%%eof"}:
        return True
    if _PDF_INTERNAL_TOKEN_PATTERN.search(lowered) and len(lowered.split()) <= 10:
        return True

    letters = sum(char.isalpha() for char in line)
    digits = sum(char.isdigit() for char in line)
    if letters == 0 and digits < 2:
        return True

    return bool(len(line) > 180 and line.count(" ") < 3)


def _normalize_candidate_lines(lines: Sequence[str]) -> list[str]:
    normalized_lines: list[str] = []
    for line in lines:
        normalized = _coerce_printable(line)
        if not normalized or _looks_like_noise_line(normalized):
            continue
        normalized_lines.append(normalized)
    return normalized_lines


def _split_pages(decoded: str) -> tuple[str, ...]:
    normalized = decoded.replace("\r\n", "\n").replace("\r", "\n")
    if "\f" in normalized:
        return tuple(page for page in normalized.split("\f") if page.strip())

    marked = _PAGE_MARKER_PATTERN.split(normalized)
    if len(marked) > 1:
        pages = [page for page in marked if page.strip()]
        if len(pages) > 1 and pages[0].lstrip().startswith("%PDF"):
            pages = pages[1:]
        return tuple(pages)
    return (normalized,)


def _extract_page_lines(page_text: str) -> list[str]:
    token_lines: list[tuple[int, str]] = []
    for match in _TEXT_TOKEN_PATTERN.finditer(page_text):
        token_lines.append(
            (
                match.start(),
                _coerce_printable(match.group("token").replace("\\(", "(").replace("\\)", ")")),
            )
        )
    for match in _TEXT_ARRAY_PATTERN.finditer(page_text):
        raw_array = match.group("array")
        string_parts = [
            part.group("token").replace("\\(", "(").replace("\\)", ")")
            for part in _STRING_TOKEN_PATTERN.finditer(raw_array)
        ]
        if not string_parts:
            continue
        token_lines.append((match.start(), _coerce_printable("".join(string_parts))))

    if token_lines:
        ordered = [line for _, line in sorted(token_lines, key=lambda row: row[0])]
        return _normalize_candidate_lines(ordered)

    return _normalize_candidate_lines(page_text.splitlines())


def _looks_like_metric_label(label: str) -> bool:
    lowered = label.lower()
    return any(hint in lowered for hint in _METRIC_HINTS)


def _metric_like_row_from_line(line: str) -> tuple[str, str] | None:
    lowered = line.lower()
    if any(token in lowered for token in (" was ", " were ", " is ", " are ", " remained ")):
        return None
    if len(line.split()) > 16:
        return None

    for hint in sorted(_METRIC_HINTS, key=len, reverse=True):
        hint_index = lowered.find(hint)
        if hint_index == -1:
            continue
        tail = line[hint_index + len(hint) :]
        value_match = _VALUE_TOKEN_PATTERN.search(tail)
        if value_match is None:
            continue
        label = line[hint_index : hint_index + len(hint)]
        value = value_match.group(0)
        if "%" in tail[value_match.start() : value_match.end() + 4] and not value.endswith("%"):
            value += "%"
        return (label, value)
    return None


def _extract_table_rows(*, page_number: int, lines: Sequence[str]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for line in lines:
        columns = [
            segment.strip() for segment in _TABLE_SPLIT_PATTERN.split(line) if segment.strip()
        ]
        label = ""
        value = ""
        if len(columns) >= 2 and _looks_like_metric_label(columns[0]):
            label, value = columns[0], columns[1]
        elif ":" in line:
            left, right = line.split(":", 1)
            if _looks_like_metric_label(left):
                label, value = left.strip(), right.strip()
        else:
            metric_like_row = _metric_like_row_from_line(line)
            if metric_like_row is not None:
                label, value = metric_like_row
        if not label or not value:
            continue
        rows.append(
            {
                "label": label,
                "value": value,
                "evidence_ref": f"p.{page_number}#table",
            }
        )
    return rows


def _extract_text_and_tables(
    *,
    page_texts: Sequence[str],
    stage_confidence: float,
) -> ParserStageOutput:
    text_blocks: list[str] = []
    text_block_refs: list[str] = []
    table_rows: list[dict[str, str]] = []

    for page_number, page_text in enumerate(page_texts, start=1):
        lines = _extract_page_lines(page_text)
        for line in lines:
            text_blocks.append(line)
            text_block_refs.append(f"p.{page_number}#text")
        table_rows.extend(_extract_table_rows(page_number=page_number, lines=lines))

    return ParserStageOutput(
        text_blocks=tuple(text_blocks),
        text_block_evidence_refs=tuple(text_block_refs),
        table_rows=tuple(dict(row) for row in table_rows),
        stage_confidence=stage_confidence,
    )


def _evaluate_stage(
    input_payload: PDFParserInput, *, stage_name: str, output: ParserStageOutput
) -> _StageCandidate:
    raw = RawFundedActuarialInput(
        source_document_id=input_payload.source_document_id,
        source_url=input_payload.source_url,
        effective_date=input_payload.effective_date,
        ingestion_date=input_payload.ingestion_date,
        default_money_unit_scale=input_payload.default_money_unit_scale,
        text_blocks=output.text_blocks,
        table_rows=tuple(dict(row) for row in output.table_rows),
        text_block_evidence_refs=output.text_block_evidence_refs,
    )
    facts, _diagnostics = extract_funded_and_actuarial_metrics(
        plan_id="parser_probe",
        plan_period="parser_probe",
        raw=raw,
    )
    available_metrics = {fact.metric_name for fact in facts}
    missing_metrics = tuple(
        metric_name
        for metric_name in FUNDED_ACTUARIAL_REQUIRED_METRICS
        if metric_name not in available_metrics
    )
    return _StageCandidate(
        stage_name=stage_name,
        output=output,
        raw=raw,
        missing_metrics=missing_metrics,
    )


def _best_partial(stage_candidates: dict[str, _StageCandidate]) -> _StageCandidate | None:
    if not stage_candidates:
        return None
    ranked = sorted(
        stage_candidates.values(),
        key=lambda candidate: (
            len(candidate.missing_metrics),
            -candidate.output.stage_confidence,
            candidate.stage_name,
        ),
    )
    return ranked[0]


def _build_text_stage(input_payload: PDFParserInput) -> ParserStageOutput:
    page_texts: tuple[str, ...] = ()
    try:
        reader = PdfReader(BytesIO(input_payload.pdf_bytes), strict=False)
        page_texts = tuple(page.extract_text() or "" for page in reader.pages)
    except Exception:  # noqa: BLE001 - decoding/parsing errors should fall through to fallback path
        page_texts = ()

    if not any(text.strip() for text in page_texts):
        decoded = input_payload.pdf_bytes.decode("latin-1", errors="ignore")
        page_texts = _split_pages(decoded)

    output = _extract_text_and_tables(page_texts=page_texts, stage_confidence=0.84)
    if input_payload.ocr_extract is None:
        return output

    # Force OCR fallback for obviously low-quality or empty extraction from native text parsing.
    has_no_signal = not output.table_rows and not output.text_blocks
    likely_noisy_decode = not output.table_rows and len(output.text_blocks) > 4_000
    if has_no_signal or likely_noisy_decode:
        return ParserStageOutput(
            text_blocks=(),
            text_block_evidence_refs=(),
            table_rows=(),
            stage_confidence=0.35,
        )
    return output


def _build_table_only_stage(input_payload: PDFParserInput) -> ParserStageOutput:
    full = _build_text_stage(input_payload)
    return ParserStageOutput(
        text_blocks=(),
        text_block_evidence_refs=(),
        table_rows=full.table_rows,
        stage_confidence=0.91,
    )


def _build_text_only_stage(input_payload: PDFParserInput) -> ParserStageOutput:
    full = _build_text_stage(input_payload)
    return ParserStageOutput(
        text_blocks=full.text_blocks,
        text_block_evidence_refs=full.text_block_evidence_refs,
        table_rows=(),
        stage_confidence=0.86,
    )


def _build_ocr_stage(input_payload: PDFParserInput) -> ParserStageOutput:
    if input_payload.ocr_extract is None:
        raise ValueError("ocr_not_configured")
    page_texts = tuple(
        text.strip()
        for text in input_payload.ocr_extract(input_payload.pdf_bytes)
        if isinstance(text, str) and text.strip()
    )
    if not page_texts:
        raise ValueError("ocr_returned_no_text")
    return _extract_text_and_tables(page_texts=page_texts, stage_confidence=0.72)


def _build_actionable_flags(
    *,
    attempts: Sequence[ParserAttempt],
    escalation: EscalationEvent | None,
    partial: _StageCandidate | None,
) -> tuple[str, ...]:
    flags: list[str] = []
    if escalation is not None:
        flags.append(escalation.reason)
    for attempt in attempts:
        if attempt.failure_reason is None:
            continue
        flags.append(f"{attempt.stage_name}:{attempt.failure_reason}")
    if partial is not None and partial.missing_metrics:
        flags.append("missing_required_metrics:" + ",".join(partial.missing_metrics))
    if any("ocr_not_configured" in flag for flag in flags):
        flags.append("configure_ocr_fallback")
    return _dedupe_non_empty(flags)


def _collect_provenance_refs(raw: RawFundedActuarialInput | None) -> tuple[str, ...]:
    if raw is None:
        return ()
    refs: list[str] = []
    refs.extend(raw.text_block_evidence_refs)
    for row in raw.table_rows:
        ref = row.get("evidence_ref", "").strip()
        if ref:
            refs.append(ref)
    return _dedupe_non_empty(refs)


def parse_pdf_to_funded_input(input_payload: PDFParserInput) -> PDFParserResult:
    """Parse pension PDF bytes into funded/actuarial extraction-ready structures."""
    stage_candidates: dict[str, _StageCandidate] = {}

    def _stage(
        name: str, parser_name: str, builder: Callable[[PDFParserInput], ParserStageOutput]
    ) -> ParserStage[_StageCandidate]:
        def _parse() -> _StageCandidate:
            candidate = _evaluate_stage(
                input_payload,
                stage_name=name,
                output=builder(input_payload),
            )
            stage_candidates[name] = candidate
            return candidate

        return ParserStage(stage_name=name, parser_name=parser_name, parse=_parse)

    ordered_stages = (
        _stage("table_primary", "pdf_table_primary", _build_table_only_stage),
        _stage("text_fallback", "pdf_text_fallback", _build_text_only_stage),
        _stage("full_fallback", "pdf_ocr_fallback", _build_ocr_stage),
    )
    outcome = run_fallback_chain(
        domain="funded",
        stages=ordered_stages,
        is_complete=lambda candidate: not candidate.missing_metrics,
    )
    selected = outcome.result if outcome.result is not None else _best_partial(stage_candidates)
    escalation = outcome.escalation
    selected_raw = selected.raw if selected is not None else None
    return PDFParserResult(
        raw=selected_raw,
        stage_name=selected.stage_name if selected is not None else None,
        stage_confidence=selected.output.stage_confidence if selected is not None else 0.0,
        attempts=outcome.attempts,
        escalation=escalation,
        escalation_required=escalation is not None,
        missing_metrics=(
            selected.missing_metrics if selected is not None else FUNDED_ACTUARIAL_REQUIRED_METRICS
        ),
        actionable_flags=_build_actionable_flags(
            attempts=outcome.attempts,
            escalation=escalation,
            partial=selected,
        ),
        provenance_refs=_collect_provenance_refs(selected_raw),
    )
