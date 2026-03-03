"""Entity lookup services for cross-plan exposure discovery."""

from pension_data.entities.lookup_service import (
    EntityExposureIndex,
    LookupExecutionTrace,
    build_entity_exposure_index,
    lookup_entity_exposures,
    resolve_canonical_entity_id,
)

__all__ = [
    "EntityExposureIndex",
    "LookupExecutionTrace",
    "build_entity_exposure_index",
    "lookup_entity_exposures",
    "resolve_canonical_entity_id",
]
