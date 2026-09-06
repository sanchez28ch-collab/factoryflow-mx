"""Unit tests for PostgreSQL transactional-outbox claims."""

from datetime import UTC, datetime, timedelta
from unittest import TestCase
from unittest.mock import MagicMock
from uuid import UUID

import psycopg

from factoryflow_batch.adapters.exceptions import (
    PostgresOutboxStoreDataError,
    PostgresOutboxStoreError,
)
from factoryflow_batch.adapters.postgres_outbox_store import (
    PostgresOutboxStore,
)

_NOW = datetime(2026, 9, 5, 7, 0, tzinfo=UTC)
_EXPIRES_AT = _NOW + timedelta(seconds=30)
_EVENT_ID = UUID("525f52e1-10f5-40a2-bbda-63cc2390881f")
_LEASE_ID = "worker-01:claim-001"


def _row() -> dict[str, object]:
    return {
        "event_id": _EVENT_ID,
        "destination_topic": "factoryflow.batch.ingested.v1",
        "partition_key": "batch-001",
        "payload": {"z": 2, "a": 1},
        "headers": {
            "manifest_schema": "manifest-v1",
            "content_type": "application/json",
        },
        "occurred_at": _NOW,
        "attempt_count": 2,
        "lease_owner": _LEASE_ID,
    }


def _store_with_rows(
    rows: list[dict[str, object]],
) -> tuple[PostgresOutboxStore, MagicMock, MagicMock]:
    pool = MagicMock()
    connection = pool.connection.return_value.__enter__.return_value
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = rows

    return PostgresOutboxStore(pool), pool, cursor


class TestPostgresOutboxStoreClaim(TestCase):
    """Verify claim ordering, encoding and controlled failures."""

    def test_claims_with_skip_locked_and_encodes_event(self) -> None:
        store, _, cursor = _store_with_rows([_row()])

        events = store.claim_pending(
            lease_id=_LEASE_ID,
            now=_NOW,
            lease_expires_at=_EXPIRES_AT,
            limit=25,
        )

        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event.event_id, _EVENT_ID)
        self.assertEqual(
            event.payload,
            b'{"a":1,"z":2}',
        )
        self.assertEqual(
            event.headers,
            (
                ("content_type", b"application/json"),
                ("manifest_schema", b"manifest-v1"),
            ),
        )
        self.assertEqual(event.attempt_count, 2)
        self.assertEqual(event.lease_id, _LEASE_ID)

        query, parameters = cursor.execute.call_args.args
        self.assertIn("FOR UPDATE SKIP LOCKED", query)
        self.assertIn("attempt_count + 1", query)
        self.assertEqual(
            parameters,
            (
                _NOW,
                _NOW,
                25,
                _LEASE_ID,
                _EXPIRES_AT,
            ),
        )

    def test_returns_empty_tuple_when_nothing_is_available(self) -> None:
        store, _, _ = _store_with_rows([])

        events = store.claim_pending(
            lease_id=_LEASE_ID,
            now=_NOW,
            lease_expires_at=_EXPIRES_AT,
            limit=100,
        )

        self.assertEqual(events, ())

    def test_rejects_invalid_claim_configuration_before_database_access(
        self,
    ) -> None:
        cases = (
            ("unsafe lease", _NOW, _EXPIRES_AT, 10),
            (_LEASE_ID, _NOW, _NOW, 10),
            (_LEASE_ID, _NOW, _EXPIRES_AT, 0),
            (_LEASE_ID, _NOW, _EXPIRES_AT, 1_001),
            (
                _LEASE_ID,
                datetime(2026, 9, 5, 7, 0),
                _EXPIRES_AT,
                10,
            ),
        )

        for lease_id, now, expires_at, limit in cases:
            with self.subTest(
                lease_id=lease_id,
                limit=limit,
            ):
                pool = MagicMock()
                store = PostgresOutboxStore(pool)

                with self.assertRaises(ValueError):
                    store.claim_pending(
                        lease_id=lease_id,
                        now=now,
                        lease_expires_at=expires_at,
                        limit=limit,
                    )

                pool.connection.assert_not_called()

    def test_rejects_corrupted_persisted_rows(self) -> None:
        corruptions = (
            ("payload", []),
            ("headers", {"content_type": 7}),
            ("attempt_count", 0),
            ("lease_owner", "another-lease"),
            ("destination_topic", "unsafe topic"),
        )

        for field_name, value in corruptions:
            with self.subTest(field_name=field_name):
                row = _row()
                row[field_name] = value
                store, _, _ = _store_with_rows([row])

                with self.assertRaisesRegex(
                    PostgresOutboxStoreDataError,
                    "violates its contract",
                ):
                    store.claim_pending(
                        lease_id=_LEASE_ID,
                        now=_NOW,
                        lease_expires_at=_EXPIRES_AT,
                        limit=10,
                    )

    def test_translates_database_error_without_leaking_details(self) -> None:
        pool = MagicMock()
        pool.connection.side_effect = psycopg.OperationalError("password=do-not-leak")
        store = PostgresOutboxStore(pool)

        with self.assertRaisesRegex(
            PostgresOutboxStoreError,
            "claim failed",
        ) as raised:
            store.claim_pending(
                lease_id=_LEASE_ID,
                now=_NOW,
                lease_expires_at=_EXPIRES_AT,
                limit=10,
            )

        self.assertNotIn("do-not-leak", str(raised.exception))
