"""JSON serialization adapter for BatchManifest."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import fields
from datetime import date, datetime
from typing import cast
from uuid import UUID

from factoryflow_batch.domain import (
    BatchManifest,
    BatchManifestValidationError,
)

_MANIFEST_FIELDS = frozenset(field.name for field in fields(BatchManifest))


def _validated_keys(payload: Mapping[str, object]) -> None:
    received = set(payload)
    missing = _MANIFEST_FIELDS - received
    unexpected = received - _MANIFEST_FIELDS

    if missing:
        field_list = ", ".join(sorted(missing))
        raise BatchManifestValidationError(f"Manifest is missing required fields: {field_list}.")

    if unexpected:
        field_list = ", ".join(sorted(unexpected))
        raise BatchManifestValidationError(f"Manifest contains unexpected fields: {field_list}.")


def _string(payload: Mapping[str, object], field_name: str) -> str:
    value = payload[field_name]

    if not isinstance(value, str):
        raise BatchManifestValidationError(f"{field_name} must be serialized as a string.")

    return value


def _integer(payload: Mapping[str, object], field_name: str) -> int:
    value = payload[field_name]

    if isinstance(value, bool) or not isinstance(value, int):
        raise BatchManifestValidationError(f"{field_name} must be serialized as an integer.")

    return value


def _uuid(payload: Mapping[str, object], field_name: str) -> UUID:
    value = _string(payload, field_name)

    try:
        return UUID(value)
    except ValueError as error:
        raise BatchManifestValidationError(f"{field_name} must be a valid UUID.") from error


def _date(payload: Mapping[str, object], field_name: str) -> date:
    value = _string(payload, field_name)

    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise BatchManifestValidationError(
            f"{field_name} must use ISO date format YYYY-MM-DD."
        ) from error


def _datetime(
    payload: Mapping[str, object],
    field_name: str,
) -> datetime:
    value = _string(payload, field_name)
    normalized = f"{value[:-1]}+00:00" if value.endswith("Z") else value

    try:
        return datetime.fromisoformat(normalized)
    except ValueError as error:
        raise BatchManifestValidationError(
            f"{field_name} must use ISO 8601 datetime format."
        ) from error


def batch_manifest_from_mapping(
    payload: Mapping[str, object],
) -> BatchManifest:
    """Deserialize and validate a manifest mapping."""

    _validated_keys(payload)

    return BatchManifest(
        manifest_version=_string(payload, "manifest_version"),
        batch_id=_uuid(payload, "batch_id"),
        idempotency_key=_string(payload, "idempotency_key"),
        source_system=_string(payload, "source_system"),
        dataset=_string(payload, "dataset"),
        schema_version=_string(payload, "schema_version"),
        logical_date=_date(payload, "logical_date"),
        source_file_name=_string(payload, "source_file_name"),
        landing_uri=_string(payload, "landing_uri"),
        content_type=_string(payload, "content_type"),
        extracted_at=_datetime(payload, "extracted_at"),
        landed_at=_datetime(payload, "landed_at"),
        record_count=_integer(payload, "record_count"),
        file_size_bytes=_integer(payload, "file_size_bytes"),
        sha256=_string(payload, "sha256"),
    )


def batch_manifest_to_mapping(
    manifest: BatchManifest,
) -> dict[str, str | int]:
    """Serialize a validated manifest to JSON-compatible values."""

    return {
        "manifest_version": manifest.manifest_version,
        "batch_id": str(manifest.batch_id),
        "idempotency_key": manifest.idempotency_key,
        "source_system": manifest.source_system,
        "dataset": manifest.dataset,
        "schema_version": manifest.schema_version,
        "logical_date": manifest.logical_date.isoformat(),
        "source_file_name": manifest.source_file_name,
        "landing_uri": manifest.landing_uri,
        "content_type": manifest.content_type,
        "extracted_at": _datetime_to_json(manifest.extracted_at),
        "landed_at": _datetime_to_json(manifest.landed_at),
        "record_count": manifest.record_count,
        "file_size_bytes": manifest.file_size_bytes,
        "sha256": manifest.sha256,
    }


def _datetime_to_json(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def loads_batch_manifest(document: str) -> BatchManifest:
    """Deserialize a JSON document into a validated manifest."""

    try:
        payload: object = json.loads(document)
    except json.JSONDecodeError as error:
        raise BatchManifestValidationError("Manifest document is not valid JSON.") from error

    if not isinstance(payload, dict):
        raise BatchManifestValidationError("Manifest document must contain a JSON object.")

    return batch_manifest_from_mapping(cast(dict[str, object], payload))


def dumps_batch_manifest(manifest: BatchManifest) -> str:
    """Serialize a manifest as deterministic UTF-8 JSON text."""

    return (
        json.dumps(
            batch_manifest_to_mapping(manifest),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
