"""Unit tests for PostgreSQL outbox state transitions."""

from datetime import UTC, datetime, timedelta
from unittest import TestCase
from unittest.mock import MagicMock
from uuid import UUID

import psycopg

from factoryflow_batch.adapters.exceptions import (
    PostgresOutboxStoreError,
)
from factoryflow_batch.adapters.postgres_outbox_store import (
    PostgresOutboxStore,
)
from factoryflow_batch.ports import OutboxLeaseLostError

_NOW = datetime(2026, 9, 5, 7, 30, tzinfo=UTC)
_EVENT_ID = UUID("e86371c6-bb58-46ac-b590-977bfcd0d3e8")
_LEASE_ID = "worker-01:transition-001"


def _store_with_result(
    result: tuple[UUID] | None,
) -> tuple[PostgresOutboxStore, MagicMock, MagicMock]:
    pool = MagicMock()
    connection = pool.connection.return_value.__enter__.return_value
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = result

    return PostgresOutboxStore(pool), pool, cursor


class TestPostgresOutboxStoreTransitions(TestCase):
    """Verify fenced success, retry and terminal updates."""

    def test_marks_event_published_and_releases_lease(self) -> None:
        store, _, cursor = _store_with_result((_EVENT_ID,))

        store.mark_published(
            event_id=_EVENT_ID,
            lease_id=_LEASE_ID,
            published_at=_NOW,
        )

        query, parameters = cursor.execute.call_args.args
        self.assertIn("published_at = %s", query)
        self.assertIn("lease_owner = NULL", query)
        self.assertIn("lease_owner = %s", query)
        self.assertEqual(parameters, (_NOW, _EVENT_ID, _LEASE_ID))

    def test_reschedules_event_and_records_sanitized_code(self) -> None:
        store, _, cursor = _store_with_result((_EVENT_ID,))
        available_at = _NOW + timedelta(seconds=10)

        store.reschedule(
            event_id=_EVENT_ID,
            lease_id=_LEASE_ID,
            available_at=available_at,
            error_code="publisher_retryable",
        )

        query, parameters = cursor.execute.call_args.args
        self.assertIn("available_at = %s", query)
        self.assertIn("last_error = %s", query)
        self.assertEqual(
            parameters,
            (
                available_at,
                "publisher_retryable",
                _EVENT_ID,
                _LEASE_ID,
            ),
        )

    def test_marks_event_dead_lettered_and_releases_lease(self) -> None:
        store, _, cursor = _store_with_result((_EVENT_ID,))

        store.mark_dead_lettered(
            event_id=_EVENT_ID,
            lease_id=_LEASE_ID,
            dead_lettered_at=_NOW,
            error_code="attempt_limit_reached",
        )

        query, parameters = cursor.execute.call_args.args
        self.assertIn("dead_lettered_at = %s", query)
        self.assertIn("lease_expires_at = NULL", query)
        self.assertEqual(
            parameters,
            (
                _NOW,
                "attempt_limit_reached",
                _EVENT_ID,
                _LEASE_ID,
            ),
        )

    def test_raises_when_expected_lease_no_longer_owns_event(self) -> None:
        store, _, _ = _store_with_result(None)

        with self.assertRaisesRegex(
            OutboxLeaseLostError,
            "expected lease",
        ):
            store.mark_published(
                event_id=_EVENT_ID,
                lease_id=_LEASE_ID,
                published_at=_NOW,
            )

    def test_translates_database_error_without_detail_leakage(self) -> None:
        pool = MagicMock()
        pool.connection.side_effect = psycopg.OperationalError("host=private password=do-not-leak")
        store = PostgresOutboxStore(pool)

        with self.assertRaisesRegex(
            PostgresOutboxStoreError,
            "transition failed",
        ) as raised:
            store.mark_published(
                event_id=_EVENT_ID,
                lease_id=_LEASE_ID,
                published_at=_NOW,
            )

        self.assertNotIn("do-not-leak", str(raised.exception))

    def test_rejects_invalid_parameters_before_database_access(self) -> None:
        pool = MagicMock()
        store = PostgresOutboxStore(pool)

        with self.assertRaises(ValueError):
            store.mark_published(
                event_id=UUID(int=0),
                lease_id=_LEASE_ID,
                published_at=_NOW,
            )

        with self.assertRaises(ValueError):
            store.reschedule(
                event_id=_EVENT_ID,
                lease_id="unsafe lease",
                available_at=_NOW,
                error_code="publisher_retryable",
            )

        with self.assertRaises(ValueError):
            store.reschedule(
                event_id=_EVENT_ID,
                lease_id=_LEASE_ID,
                available_at=datetime(2026, 9, 5, 7, 30),
                error_code="contains secret details!",
            )

        pool.connection.assert_not_called()
