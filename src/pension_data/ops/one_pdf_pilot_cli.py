"""CLI entrypoint for one-PDF pilot orchestration runs."""

from __future__ import annotations

import argparse
import json
import sys

from pension_data.ops.one_pdf_pilot import (
    one_pdf_pilot_input_contract,
    resolve_one_pdf_pilot_input,
    resolve_one_pdf_pilot_runtime_options,
    run_one_pdf_pilot,
)


def _build_parser() -> argparse.ArgumentParser:
    contract = one_pdf_pilot_input_contract()
    env_var_by_field = contract["env_var_by_field"]
    assert isinstance(env_var_by_field, dict)

    parser = argparse.ArgumentParser(
        description="Run one deterministic parser/orchestration pilot for a single pension PDF.",
    )
    parser.add_argument(
        "--pdf-path",
        default=None,
        help=f"Path to source PDF (or ${env_var_by_field['pdf_path']}).",
    )
    parser.add_argument(
        "--plan-id",
        default=None,
        help=f"Plan identifier (or ${env_var_by_field['plan_id']}).",
    )
    parser.add_argument(
        "--plan-period",
        default=None,
        help=f"Plan period label (or ${env_var_by_field['plan_period']}).",
    )
    parser.add_argument(
        "--effective-date",
        default=None,
        help=f"Effective date YYYY-MM-DD (or ${env_var_by_field['effective_date']}).",
    )
    parser.add_argument(
        "--ingestion-date",
        default=None,
        help=f"Ingestion date YYYY-MM-DD (or ${env_var_by_field['ingestion_date']}).",
    )
    parser.add_argument(
        "--output-root",
        default=None,
        help=f"Output root dir (default: outputs, or ${env_var_by_field['output_root']}).",
    )
    parser.add_argument(
        "--source-url",
        default=None,
        help=f"Optional canonical source URL (or ${env_var_by_field['source_url']}).",
    )
    parser.add_argument(
        "--source-document-id",
        default=None,
        help=f"Optional source document ID override (or ${env_var_by_field['source_document_id']}).",
    )
    parser.add_argument(
        "--fetched-at",
        default=None,
        help=f"Optional fetched timestamp ISO8601 (or ${env_var_by_field['fetched_at']}).",
    )
    parser.add_argument(
        "--mime-type",
        default=None,
        help=f"MIME type (default: application/pdf, or ${env_var_by_field['mime_type']}).",
    )
    parser.add_argument(
        "--default-money-unit-scale",
        default=None,
        help=(
            "Default money unit scale "
            f"(usd|thousand_usd|million_usd|billion_usd, or "
            f"${env_var_by_field['default_money_unit_scale']})."
        ),
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help=f"Optional deterministic run ID override (or ${env_var_by_field['run_id']}).",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    try:
        pilot_input = resolve_one_pdf_pilot_input(
            pdf_path=args.pdf_path,
            plan_id=args.plan_id,
            plan_period=args.plan_period,
            effective_date=args.effective_date,
            ingestion_date=args.ingestion_date,
            default_money_unit_scale=args.default_money_unit_scale,
            source_url=args.source_url,
            source_document_id=args.source_document_id,
            fetched_at=args.fetched_at,
            mime_type=args.mime_type,
        )
        output_root, run_id = resolve_one_pdf_pilot_runtime_options(
            output_root=args.output_root,
            run_id=args.run_id,
        )
        result = run_one_pdf_pilot(
            pilot_input=pilot_input,
            output_root=output_root,
            run_id=run_id,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"one-pdf pilot failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
