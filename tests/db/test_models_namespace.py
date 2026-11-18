"""Tests for lazy exports on ``pension_data.db.models``."""

from __future__ import annotations

import pension_data.quality.confidence  # noqa: F401 - breaks review_queue import cycle

import pytest

import pension_data.db.models as models


def test_models_namespace_lazy_loads_review_queue_types() -> None:
    lazy_record = models.ExtractionReviewQueueRecord
    lazy_audit = models.ReviewQueueAuditEntry
    from pension_data.db.models import review_queue

    assert lazy_record is review_queue.ExtractionReviewQueueRecord
    assert lazy_audit is review_queue.ReviewQueueAuditEntry


def test_models_namespace_unknown_attribute_raises() -> None:
    with pytest.raises(AttributeError, match=r"has no attribute 'NotARealModel'"):
        _ = models.NotARealModel
