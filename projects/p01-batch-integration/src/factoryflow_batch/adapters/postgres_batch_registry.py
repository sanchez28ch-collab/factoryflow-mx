"""PostgreSQL implementation of the batch registry and transactional outbox."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Final, cast
from uuid import NAMESPACE_URL, UUID, uuid5

import psycopg
from psycopg import Connection, Cursor
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from factoryflow_batch.adapters.exceptions import (
    PostgresBatchRegistryDataError,
    PostgresBatchRegistryError,
)
from factoryflow_batch.adapters.manifest_json import batch_manifest_to_mapping
from factoryflow_batch.domain import (
    BatchManifest,
    BatchManifestValidationError,
)
from factoryflow_batch.ports import BatchRegistration

_EVENT_TYPE: Final = "factoryflow.batch.ingested"
_EVENT_VERSION: Final = 1
_DESTINATION_TOPIC: Final = "factoryflow.batch.ingested.v1"
_EVENT_SCHEMA: Final = "urn:factoryflow:events:batch-ingested:1.0.0"
_MANIFEST_SCHEMA: Final = "urn:factoryflow:contracts:batch-manifest:1.0.0"
_IDEMPOTENCY_KEY_PATTERN: Final = re.compile(r"^[a-f0-9]{64}$")

_MANIFEST_COLUMNS = """
    batch_id,
    idempotency_key,
    manifest_version,
    source_system,
    dataset,
    schema_version,
    logical_date,
    source_file_name,
    landing_uri,
    content_type,
    extracted_at,
    landed_at,
    record_count,
    file_size_bytes,
    sha256
"""

_SELECT_BY_IDEMPOTENCY_KEY = f"""
SELECT
{_MANIFEST_COLUMNS}
FROM ingestion.batch_manifests
WHERE idempotency_key = %s
"""

_INSERT_MANIFEST = f"""
INSERT INTO ingestion.batch_manifests (
{_MANIFEST_COLUMNS}
)
VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s
)
ON CONFLICT DO NOTHING
RETURNING
{_MANIFEST_COLUMNS}
"""

_INSERT_OUTBOX_EVENT = """
INSERT INTO ingestion.outbox_events (
    event_id,
    aggregate_type,
    aggregate_id,
    event_type,
    event_version,
    destination_topic,
    partition_key,
    payload,
    headers,
    occurred_at
)
VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s
)
"""


class PostgresBatchRegistry:
    """Persist manifests and their integration event atomically."""

    def __init__(
        self,
        pool: ConnectionPool[Connection[Any]],
    ) -> None:
        self._pool = pool

    def find_by_idempotency_key(
        self,
        idempotency_key: str,
        /,
    ) -> BatchManifest | None:
        """Return the persisted manifest associated with an idempotency key."""

        _validate_idempotency_key(idempotency_key)

        try:
            with (
                self._pool.connection() as connection,
                connection.cursor(row_factory=dict_row) as cursor,
            ):
                cursor.execute(
                    _SELECT_BY_IDEMPOTENCY_KEY,
                    (idempotency_key,),
                )
                row = cursor.fetchone()
        except psycopg.Error as exc:
            raise PostgresBatchRegistryError("PostgreSQL batch lookup failed.") from exc

        if row is None:
            return None

        return _manifest_from_row(row)

    def register_if_absent(
        self,
        manifest: BatchManifest,
        /,
    ) -> BatchRegistration:
        """Insert one manifest and outbox event or return the concurrent winner."""

        try:
            with (
                self._pool.connection() as connection,
                connection.transaction(),
                connection.cursor(row_factory=dict_row) as cursor,
            ):
                cursor.execute(
                    _INSERT_MANIFEST,
                    _manifest_values(manifest),
                )
                inserted_row = cursor.fetchone()

                if inserted_row is not None:
                    persisted = _manifest_from_row(inserted_row)
                    self._insert_outbox_event(
                        cursor=cursor,
                        manifest=persisted,
                    )
                    return BatchRegistration(
                        created=True,
                        manifest=persisted,
                    )

                cursor.execute(
                    _SELECT_BY_IDEMPOTENCY_KEY,
                    (manifest.idempotency_key,),
                )
                existing_row = cursor.fetchone()

                if existing_row is None:
                    raise PostgresBatchRegistryDataError(
                        "The conflicting batch registration could not be read."
                    )

                return BatchRegistration(
                    created=False,
                    manifest=_manifest_from_row(existing_row),
                )
        except PostgresBatchRegistryDataError:
            raise
        except psycopg.Error as exc:
            raise PostgresBatchRegistryError("PostgreSQL batch registration failed.") from exc

    @staticmethod
    def _insert_outbox_event(
        *,
        cursor: Cursor[Any],
        manifest: BatchManifest,
    ) -> None:
        event_id = _event_id_for(manifest)
        occurred_at = _utc_text(manifest.landed_at)

        payload: dict[str, object] = {
            "event_id": str(event_id),
            "event_type": _EVENT_TYPE,
            "event_version": _EVENT_VERSION,
            "occurred_at": occurred_at,
            "aggregate_type": "batch",
            "aggregate_id": str(manifest.batch_id),
            "data": batch_manifest_to_mapping(manifest),
        }
        headers: dict[str, object] = {
            "content_type": "application/json",
            "event_schema": _EVENT_SCHEMA,
            "manifest_schema": _MANIFEST_SCHEMA,
        }

        cursor.execute(
            _INSERT_OUTBOX_EVENT,
            (
                event_id,
                "batch",
                manifest.batch_id,
                _EVENT_TYPE,
                _EVENT_VERSION,
                _DESTINATION_TOPIC,
                str(manifest.batch_id),
                Jsonb(payload),
                Jsonb(headers),
                manifest.landed_at,
            ),
        )


def _validate_idempotency_key(idempotency_key: str) -> None:
    if (
        not isinstance(idempotency_key, str)
        or _IDEMPOTENCY_KEY_PATTERN.fullmatch(idempotency_key) is None
    ):
        raise ValueError("idempotency_key must be a lowercase hexadecimal SHA-256 value.")


def _manifest_values(manifest: BatchManifest) -> tuple[object, ...]:
    return (
        manifest.batch_id,
        manifest.idempotency_key,
        manifest.manifest_version,
        manifest.source_system,
        manifest.dataset,
        manifest.schema_version,
        manifest.logical_date,
        manifest.source_file_name,
        manifest.landing_uri,
        manifest.content_type,
        manifest.extracted_at,
        manifest.landed_at,
        manifest.record_count,
        manifest.file_size_bytes,
        manifest.sha256,
    )


def _manifest_from_row(
    row: Mapping[str, object],
) -> BatchManifest:
    try:
        return BatchManifest(
            batch_id=cast(UUID, row["batch_id"]),
            idempotency_key=cast(str, row["idempotency_key"]),
            manifest_version=cast(str, row["manifest_version"]),
            source_system=cast(str, row["source_system"]),
            dataset=cast(str, row["dataset"]),
            schema_version=cast(str, row["schema_version"]),
            logical_date=cast(Any, row["logical_date"]),
            source_file_name=cast(str, row["source_file_name"]),
            landing_uri=cast(str, row["landing_uri"]),
            content_type=cast(str, row["content_type"]),
            extracted_at=cast(datetime, row["extracted_at"]),
            landed_at=cast(datetime, row["landed_at"]),
            record_count=cast(int, row["record_count"]),
            file_size_bytes=cast(int, row["file_size_bytes"]),
            sha256=cast(str, row["sha256"]),
        )
    except (
        BatchManifestValidationError,
        KeyError,
        TypeError,
        ValueError,
    ) as exc:
        raise PostgresBatchRegistryDataError(
            "Persisted batch manifest violates the domain contract."
        ) from exc


def _event_id_for(manifest: BatchManifest) -> UUID:
    return uuid5(
        NAMESPACE_URL,
        (f"urn:factoryflow:event:{_EVENT_TYPE}:{_EVENT_VERSION}:{manifest.batch_id}"),
    )


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
