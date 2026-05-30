"""Generate the first static UI/LangChain reviewable findings artifact."""

from __future__ import annotations

import argparse
import sys

from pension_data.langchain.review_artifact import (
    REVIEWABLE_FINDINGS_ARTIFACT_PATH,
    REVIEWABLE_FINDINGS_FIRST_SLICE_ID,
    ReviewableFindingsArtifactError,
    build_extraction_quality_dashboard_artifact,
    write_reviewable_findings_artifact,
)

DEFAULT_PERSISTENCE_CONTRACT_PATH = "extraction_persistence/persistence_contract.json"
DEFAULT_READINESS_CSV_PATH = "coverage/readiness_rows.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate the extraction quality dashboard reviewable findings artifact."
    )
    parser.add_argument(
        "--slice",
        default=REVIEWABLE_FINDINGS_FIRST_SLICE_ID,
        choices=[REVIEWABLE_FINDINGS_FIRST_SLICE_ID],
        help="Artifact slice to generate. Currently only extraction_quality_dashboard is supported.",
    )
    parser.add_argument(
        "--output",
        default=REVIEWABLE_FINDINGS_ARTIFACT_PATH,
        help="Output path for the artifact JSON.",
    )
    parser.add_argument(
        "--generated-at",
        default=None,
        help="Override generated_at timestamp (ISO-8601, UTC preferred).",
    )
    parser.add_argument(
        "--artifact-date",
        default=None,
        help="Override artifact date used in artifact_id (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--persistence-contract",
        default=DEFAULT_PERSISTENCE_CONTRACT_PATH,
        help=(
            "Path to the extraction persistence contract JSON written by "
            "write_extraction_persistence_artifacts()."
        ),
    )
    parser.add_argument(
        "--readiness-csv",
        default=DEFAULT_READINESS_CSV_PATH,
        help=(
            "Path to the readiness CSV written by write_coverage_artifacts() "
            "(or equivalent source-authority readiness output)."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        artifact = build_extraction_quality_dashboard_artifact(
            generated_at=args.generated_at,
            artifact_date=args.artifact_date,
            persistence_contract_path=args.persistence_contract,
            readiness_csv_path=args.readiness_csv,
        )
        path = write_reviewable_findings_artifact(artifact, output_path=args.output)
    except ReviewableFindingsArtifactError as exc:
        print(f"ReviewableFindingsArtifactError: {exc}", file=sys.stderr)
        return 1
    print(path.as_posix())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
