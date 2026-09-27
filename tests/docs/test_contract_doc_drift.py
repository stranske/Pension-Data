"""Guardrails to keep run-contract documentation aligned with shipped emitters."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUN_CONTRACT_DOC = ROOT / "docs" / "contracts" / "run-contract-v1.md"

STALE_NO_EMITTER_CLAIM = "No participant emits an envelope yet"


def test_run_contract_doc_mentions_pension_data_emitter() -> None:
    text = RUN_CONTRACT_DOC.read_text(encoding="utf-8")
    assert STALE_NO_EMITTER_CLAIM not in text
    assert "stranske/Pension-Data" in text
    assert "build_backplane_reference_run" in text
    assert "backplane_emitter.py" in text
    assert "config/backplane_participants.json" in text
