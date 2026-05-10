"""Generate the first static UI/LangChain reviewable findings artifact."""

from __future__ import annotations

import argparse

from pension_data.langchain.review_artifact import (
    REVIEWABLE_FINDINGS_ARTIFACT_PATH,
    build_extraction_quality_dashboard_artifact,
    write_reviewable_findings_artifact,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate the extraction quality dashboard reviewable findings artifact."
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
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    artifact = build_extraction_quality_dashboard_artifact(
        generated_at=args.generated_at,
        artifact_date=args.artifact_date,
    )
    path = write_reviewable_findings_artifact(artifact, output_path=args.output)
    print(path.as_posix())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
