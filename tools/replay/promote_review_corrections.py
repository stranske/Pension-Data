"""Promote reviewed extraction corrections into a replay baseline with provenance."""

from __future__ import annotations

import argparse
import copy
import json
import sys
from pathlib import Path
from typing import Any

from tools.replay.harness import load_snapshot


def _load_corrections(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("corrections") if isinstance(payload, dict) else payload
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError("corrections must be a list or an object with a corrections list")
    return rows


def promote(*, baseline_path: Path, corrections_path: Path, output_path: Path) -> int:
    """Promote each resolved correction once, refusing unknown or duplicate targets."""
    baseline = copy.deepcopy(load_snapshot(baseline_path))
    provenance = baseline.setdefault("correction_provenance", [])
    if not isinstance(provenance, list):  # defensive; load_snapshot already checks persisted files
        raise ValueError("baseline correction provenance is malformed")
    known_ids = {
        row.get("correction_id")
        for row in provenance
        if isinstance(row, dict) and row.get("correction_id")
    }
    documents = {row["document_id"]: row for row in baseline["documents"]}

    for row in _load_corrections(corrections_path):
        correction_id = row.get("correction_id")
        document_id = row.get("document_id")
        field = row.get("field")
        reviewer = row.get("reviewer")
        evidence_refs = row.get("evidence_refs")
        if not all(
            isinstance(value, str) and value.strip()
            for value in (correction_id, document_id, field, reviewer)
        ):
            raise ValueError(
                "each correction needs non-empty correction_id, document_id, field, and reviewer"
            )
        if row.get("state") != "resolved":
            raise ValueError(f"correction '{correction_id}' is not resolved by review")
        if not isinstance(evidence_refs, list) or not all(
            isinstance(ref, str) and ref for ref in evidence_refs
        ):
            raise ValueError(f"correction '{correction_id}' needs non-empty evidence_refs")
        if correction_id in known_ids:
            raise ValueError(f"correction '{correction_id}' was already promoted")
        document = documents.get(document_id)
        if document is None or field not in document["fields"]:
            raise ValueError(f"correction '{correction_id}' does not match a golden field")
        document["fields"][field]["value"] = row.get("value")
        provenance.append(
            {
                "correction_id": correction_id,
                "document_id": document_id,
                "field": field,
                "reviewer": reviewer,
                "evidence_refs": evidence_refs,
            }
        )
        known_ids.add(correction_id)

    output_path.write_text(json.dumps(baseline, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return len(known_ids)


def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--corrections", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--baseline-update-ticket", required=True)
    args = parser.parse_args(argv)
    try:
        promoted_count = promote(
            baseline_path=args.baseline,
            corrections_path=args.corrections,
            output_path=args.output,
        )
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        print(f"review-correction promotion error: {exc}", file=sys.stderr)
        return 1
    print(
        f"Promoted review corrections into {args.output} ({promoted_count} total provenance rows)"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(run())
