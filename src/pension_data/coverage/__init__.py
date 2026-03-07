"""Coverage and readiness outputs for extraction gating."""

from pension_data.coverage.component_completeness import (
    CORE_SCHEMA_COMPONENTS,
    build_component_coverage_report_from_manifest,
    build_component_datasets,
    load_component_datasets_from_manifest,
    validate_component_coverage,
)
from pension_data.coverage.readiness import (
    build_publication_artifacts,
    build_readiness_artifacts,
    write_coverage_artifacts,
)

__all__ = [
    "CORE_SCHEMA_COMPONENTS",
    "build_component_coverage_report_from_manifest",
    "build_component_datasets",
    "build_publication_artifacts",
    "build_readiness_artifacts",
    "load_component_datasets_from_manifest",
    "validate_component_coverage",
    "write_coverage_artifacts",
]
