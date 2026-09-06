"""PostgreSQL leasing and state transitions for transactional outbox events."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from factoryflow_batch.adapters.exceptions import (
    PostgresOutboxStoreDataError,
    PostgresOutboxStoreError,
)
from factoryflow_batch.ports import (
    LeasedOutboxEvent,
    OutboxLeaseLostError,
)

_LEASE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_ERROR_CODE_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_TOPIC_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")

_CLAIM_PENDING = """
WITH candidates AS MATERIALIZED (
    SELECT event_id
    FROM ingestion.outbox_events
    WHERE published_at IS NULL
      AND dead_lettered_at IS NULL
      AND available_at <= %s
      AND (
          lease_owner IS NULL
          OR lease_expires_at <= %s
      )
    ORDER BY available_at, created_at, event_id
    FOR UPDATE SKIP LOCKED
    LIMIT %s
),
claimed AS (
    UPDATE ingestion.outbox_events AS event
    SET
        lease_owner = %s,
        lease_expires_at = %s,
        attempt_count = event.attempt_count + 1
    FROM candidates
    WHERE event.event_id = candidates.event_id
    RETURNING
        event.event_id,
        event.destination_topic,
        event.partition_key,
        event.payload,
        event.headers,
        event.occurred_at,
        event.attempt_count,
        event.lease_owner,
        event.available_at,
        event.created_at
)
SELECT
    event_id,
    destination_topic,
    partition_key,
    payload,
    headers,
    occurred_at,
    attempt_count,
    lease_owner
FROM claimed
ORDER BY available_at, created_at, event_id
"""

_MARK_PUBLISHED = """
UPDATE ingestion.outbox_events
SET
    published_at = %s,
    last_error = NULL,
    lease_owner = NULL,
    lease_expires_at = NULL
WHERE event_id = %s
  AND lease_owner = %s
  AND published_at IS NULL
  AND dead_lettered_at IS NULL
RETURNING event_id
"""

_RESCHEDULE = """
UPDATE ingestion.outbox_events
SET
    available_at = %s,
    last_error = %s,
    lease_owner = NULL,
    lease_expires_at = NULL
WHERE event_id = %s
  AND lease_owner = %s
  AND published_at IS NULL
  AND dead_lettered_at IS NULL
RETURNING event_id
"""

_MARK_DEAD_LETTERED = """
UPDATE ingestion.outbox_events
SET
    dead_lettered_at = %s,
    last_error = %s,
    lease_owner = NULL,
    lease_expires_at = NULL
WHERE event_id = %s
  AND lease_owner = %s
  AND published_at IS NULL
  AND dead_lettered_at IS NULL
RETURNING event_id
"""


class PostgresOutboxStore:
    """Claim and transition outbox rows using fenced lease identifiers."""

    def __init__(
        self,
        pool: ConnectionPool[Connection[Any]],
    ) -> None:
        self._pool = pool

    def claim_pending(
        self,
        *,
        lease_id: str,
        now: datetime,
        lease_expires_at: datetime,
        limit: int,
    ) -> tuple[LeasedOutboxEvent, ...]:
        """Claim available rows without blocking competing workers."""

        _validate_lease_id(lease_id)
        claimed_at = _aware_utc(name="now", value=now)
        expires_at = _aware_utc(
            name="lease_expires_at",
            value=lease_expires_at,
        )
        _validate_limit(limit)

        if expires_at <= claimed_at:
            raise ValueError("lease_expires_at must be later than now.")

        try:
            with (
                self._pool.connection() as connection,
                connection.transaction(),
                connection.cursor(row_factory=dict_row) as cursor,
            ):
                cursor.execute(
                    _CLAIM_PENDING,
                    (
                        claimed_at,
                        claimed_at,
                        limit,
                        lease_id,
                        expires_at,
                    ),
                )
                rows = cursor.fetchall()
        except psycopg.Error as exc:
            raise PostgresOutboxStoreError("PostgreSQL outbox claim failed.") from exc

        return tuple(_event_from_row(row, expected_lease_id=lease_id) for row in rows)

    def mark_published(
        self,
        *,
        event_id: UUID,
        lease_id: str,
        published_at: datetime,
    ) -> None:
        """Commit broker acknowledgement under the current lease."""

        self._transition(
            query=_MARK_PUBLISHED,
            parameters=(
                _aware_utc(name="published_at", value=published_at),
                _validate_event_id(event_id),
                _validated_lease_id(lease_id),
            ),
        )

    def reschedule(
        self,
        *,
        event_id: UUID,
        lease_id: str,
        available_at: datetime,
        error_code: str,
    ) -> None:
        """Release an event for a future retry."""

        self._transition(
            query=_RESCHEDULE,
            parameters=(
                _aware_utc(name="available_at", value=available_at),
                _validated_error_code(error_code),
                _validate_event_id(event_id),
                _validated_lease_id(lease_id),
            ),
        )

    def mark_dead_lettered(
        self,
        *,
        event_id: UUID,
        lease_id: str,
        dead_lettered_at: datetime,
        error_code: str,
    ) -> None:
        """Commit terminal exhaustion and release the lease."""

        self._transition(
            query=_MARK_DEAD_LETTERED,
            parameters=(
                _aware_utc(
                    name="dead_lettered_at",
                    value=dead_lettered_at,
                ),
                _validated_error_code(error_code),
                _validate_event_id(event_id),
                _validated_lease_id(lease_id),
            ),
        )

    def _transition(
        self,
        *,
        query: str,
        parameters: tuple[object, ...],
    ) -> None:
        try:
            with (
                self._pool.connection() as connection,
                connection.transaction(),
                connection.cursor() as cursor,
            ):
                cursor.execute(query, parameters)
                transitioned = cursor.fetchone()
        except psycopg.Error as exc:
            raise PostgresOutboxStoreError("PostgreSQL outbox transition failed.") from exc

        if transitioned is None:
            raise OutboxLeaseLostError("The outbox event is not owned by the expected lease.")


def _event_from_row(
    row: Mapping[str, object],
    *,
    expected_lease_id: str,
) -> LeasedOutboxEvent:
    try:
        event_id = _validate_event_id(row["event_id"])
        topic = row["destination_topic"]
        partition_key = row["partition_key"]
        attempt_count = row["attempt_count"]
        lease_owner = row["lease_owner"]

        if not isinstance(topic, str) or not _TOPIC_PATTERN.fullmatch(topic):
            raise ValueError("invalid destination topic")
        if (
            not isinstance(partition_key, str)
            or not partition_key
            or partition_key != partition_key.strip()
        ):
            raise ValueError("invalid partition key")
        if (
            isinstance(attempt_count, bool)
            or not isinstance(attempt_count, int)
            or attempt_count < 1
        ):
            raise ValueError("invalid attempt count")
        if lease_owner != expected_lease_id:
            raise ValueError("unexpected lease owner")

        return LeasedOutboxEvent(
            event_id=event_id,
            destination_topic=topic,
            partition_key=partition_key,
            payload=_encode_payload(row["payload"]),
            headers=_encode_headers(row["headers"]),
            occurred_at=_aware_utc(
                name="occurred_at",
                value=row["occurred_at"],
            ),
            attempt_count=attempt_count,
            lease_id=expected_lease_id,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise PostgresOutboxStoreDataError("Persisted outbox event violates its contract.") from exc


def _encode_payload(value: object) -> bytes:
    payload = _json_object(value)

    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _encode_headers(value: object) -> tuple[tuple[str, bytes], ...]:
    headers = _json_object(value)
    encoded: list[tuple[str, bytes]] = []

    for key, header_value in headers.items():
        if not isinstance(header_value, str):
            raise ValueError("outbox header values must be strings")
        encoded.append((key, header_value.encode("utf-8")))

    return tuple(sorted(encoded))


def _json_object(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise ValueError("value must be a JSON object")

    return cast(Mapping[str, object], value)


def _aware_utc(*, name: str, value: object) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be a timezone-aware datetime.")

    return value.astimezone(UTC)


def _validate_limit(limit: int) -> None:
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1_000:
        raise ValueError("limit must be an integer between 1 and 1000.")


def _validate_event_id(event_id: object) -> UUID:
    if not isinstance(event_id, UUID) or event_id.int == 0:
        raise ValueError("event_id must be a non-zero UUID.")

    return event_id


def _validate_lease_id(lease_id: str) -> None:
    if not isinstance(lease_id, str) or not _LEASE_PATTERN.fullmatch(lease_id):
        raise ValueError("lease_id uses an unsupported format.")


def _validated_lease_id(lease_id: str) -> str:
    _validate_lease_id(lease_id)
    return lease_id


def _validated_error_code(error_code: str) -> str:
    if not isinstance(error_code, str) or not _ERROR_CODE_PATTERN.fullmatch(error_code):
        raise ValueError("error_code uses an unsupported format.")

    return error_code
