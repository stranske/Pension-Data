"""Shared extraction helpers used across parser modules."""

from pension_data.extract.common.evidence import (
    build_evidence_reference,
    canonicalize_evidence_ref,
    table_evidence_ref,
    text_block_evidence_ref,
)
from pension_data.extract.common.ids import stable_id

__all__ = [
    "build_evidence_reference",
    "canonicalize_evidence_ref",
    "stable_id",
    "table_evidence_ref",
    "text_block_evidence_ref",
]
