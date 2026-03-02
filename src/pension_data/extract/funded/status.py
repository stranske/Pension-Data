"""Funded-status extraction helpers built on funded/actuarial parser outputs."""

from __future__ import annotations

from pension_data.db.models.funded_actuarial import (
    ExtractionDiagnostic,
    FundedActuarialStagingFact,
)
from pension_data.extract.actuarial.metrics import (
    RawFundedActuarialInput,
    extract_funded_and_actuarial_metrics,
)

_FUNDED_METRICS: tuple[str, ...] = ("funded_ratio", "aal_usd", "ava_usd")


def extract_funded_status_metrics(
    *,
    plan_id: str,
    plan_period: str,
    raw: RawFundedActuarialInput,
) -> tuple[list[FundedActuarialStagingFact], list[ExtractionDiagnostic]]:
    """Extract funded-status subset metrics while preserving parser diagnostics."""
    facts, diagnostics = extract_funded_and_actuarial_metrics(
        plan_id=plan_id,
        plan_period=plan_period,
        raw=raw,
    )
    funded_facts = [fact for fact in facts if fact.metric_name in _FUNDED_METRICS]
    funded_diagnostics = [
        item
        for item in diagnostics
        if item.metric_name in _FUNDED_METRICS or item.code == "ambiguous_metric"
    ]
    return funded_facts, funded_diagnostics
