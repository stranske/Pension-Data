#!/usr/bin/env python3
"""CLI entrypoint for one-PDF pilot orchestration runs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import cast

from pension_data.normalize.financial_units import UnitScale
from pension_data.ops.one_pdf_pilot import OnePdfPilotInput, run_one_pdf_pilot


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one deterministic parser/orchestration pilot for a single pension PDF.",
    )
    parser.add_argument("--pdf-path", required=True, help="Path to the source PDF file.")
    parser.add_argument("--plan-id", required=True, help="Plan identifier for the run.")
    parser.add_argument("--plan-period", required=True, help="Plan period label (e.g. FY2024).")
    parser.add_argument(
        "--effective-date",
        required=True,
        help="Effective date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--ingestion-date",
        required=True,
        help="Ingestion date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--output-root",
        default="outputs",
        help="Output root directory for pilot artifacts (default: outputs).",
    )
    parser.add_argument("--source-url", default=None, help="Optional canonical source URL.")
    parser.add_argument(
        "--source-document-id",
        default=None,
        help="Optional source document ID override.",
    )
    parser.add_argument(
        "--fetched-at",
        default=None,
        help="Optional fetched timestamp in ISO8601 format.",
    )
    parser.add_argument(
        "--mime-type",
        default="application/pdf",
        help="MIME type for the input payload (default: application/pdf).",
    )
    parser.add_argument(
        "--default-money-unit-scale",
        default="million_usd",
        choices=["usd", "thousand_usd", "million_usd", "billion_usd"],
        help="Default money unit scale for parser normalization.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional deterministic run ID override.",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    try:
        result = run_one_pdf_pilot(
            pilot_input=OnePdfPilotInput(
                pdf_path=Path(args.pdf_path),
                plan_id=args.plan_id,
                plan_period=args.plan_period,
                effective_date=args.effective_date,
                ingestion_date=args.ingestion_date,
                default_money_unit_scale=cast(UnitScale, args.default_money_unit_scale),
                source_url=args.source_url,
                source_document_id=args.source_document_id,
                fetched_at=args.fetched_at,
                mime_type=args.mime_type,
            ),
            output_root=Path(args.output_root),
            run_id=args.run_id,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"one-pdf pilot failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
