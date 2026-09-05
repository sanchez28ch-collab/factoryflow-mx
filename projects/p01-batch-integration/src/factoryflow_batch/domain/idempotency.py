"""Deterministic identities used to make batch ingestion idempotent."""

from __future__ import annotations

import hashlib
from datetime import date

from factoryflow_batch.domain.validation import (
    validated_logical_date,
    validated_name,
    validated_sha256,
    validated_version,
)

_FIELD_SEPARATOR = "\x1f"


def build_batch_idempotency_key(
    *,
    source_system: str,
    dataset: str,
    logical_date: date,
    schema_version: str,
    artifact_sha256: str,
) -> str:
    """Build the stable identity of one logical batch artifact."""

    valid_source = validated_name(source_system, "source_system")
    valid_dataset = validated_name(dataset, "dataset")
    valid_date = validated_logical_date(logical_date)
    valid_version = validated_version(schema_version, "schema_version")
    valid_sha256 = validated_sha256(
        artifact_sha256,
        "artifact_sha256",
    )

    canonical_identity = _FIELD_SEPARATOR.join(
        (
            valid_source,
            valid_dataset,
            valid_date.isoformat(),
            valid_version,
            valid_sha256,
        )
    )

    return hashlib.sha256(canonical_identity.encode("utf-8")).hexdigest()
