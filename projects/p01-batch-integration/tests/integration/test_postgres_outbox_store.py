"""Integration tests for PostgreSQL transactional-outbox leasing."""

import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest import TestCase, skipUnless
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from factoryflow_batch.adapters.postgres_batch_registry import (
    PostgresBatchRegistry,
)
from factoryflow_batch.adapters.postgres_outbox_store import (
    PostgresOutboxStore,
)
from factoryflow_batch.domain import BatchManifest
from factoryflow_batch.ports import OutboxLeaseLostError

_DATABASE_URL = os.environ.get("P01_TEST_DATABASE_URL")
_NOW = datetime(2026, 9, 5, 8, 0, tzinfo=UTC)


def _manifest() -> BatchManifest:
    batch_id = uuid4()

    return BatchManifest.create(
        batch_id=batch_id,
        source_system="erp",
        dataset="publisher_orders",
        schema_version="1.0.0",
        logical_date=date(2026, 9, 5),
        source_file_name=f"{batch_id}.csv",
        landing_uri=f"file:///landing/{batch_id}.csv",
        content_type="text/csv",
        extracted_at=_NOW,
        landed_at=_NOW + timedelta(minutes=1),
        record_count=2,
        file_size_bytes=42,
        sha256="b" * 64,
    )


@skipUnless(
    _DATABASE_URL is not None,
    "P01_TEST_DATABASE_URL is required for PostgreSQL integration tests.",
)
class TestPostgresOutboxStoreIntegration(TestCase):
    """Exercise leases and transitions against PostgreSQL."""

    pool: ConnectionPool

    @classmethod
    def setUpClass(cls) -> None:
        assert _DATABASE_URL is not None

        project_root = Path(__file__).resolve().parents[2]
        migration = (
            project_root / "sql" / "migrations" / "001_create_batch_registry_outbox.sql"
        ).read_text(encoding="utf-8")

        with psycopg.connect(_DATABASE_URL) as connection:
            connection.execute(migration, prepare=False)

        cls.pool = ConnectionPool(
            conninfo=_DATABASE_URL,
            min_size=1,
            max_size=8,
            timeout=10,
            kwargs={
                "autocommit": True,
                "application_name": "factoryflow-p01-outbox-tests",
            },
            open=False,
        )
        cls.pool.open(wait=True, timeout=15)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.pool.close()

    def setUp(self) -> None:
        self._truncate()

    def tearDown(self) -> None:
        self._truncate()

    def _truncate(self) -> None:
        with self.pool.connection() as connection:
            connection.execute(
                """
                TRUNCATE TABLE
                    ingestion.outbox_events,
                    ingestion.batch_manifests
                """
            )

    def _register_event(self) -> None:
        PostgresBatchRegistry(self.pool).register_if_absent(_manifest())

        with self.pool.connection() as connection:
            connection.execute(
                """
                UPDATE ingestion.outbox_events
                SET available_at = %s
                """,
                (_NOW,),
            )

    def _state(self) -> dict[str, object]:
        with (
            self.pool.connection() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT
                    event_id,
                    attempt_count,
                    lease_owner,
                    lease_expires_at,
                    published_at,
                    dead_lettered_at,
                    last_error
                FROM ingestion.outbox_events
                """
            )
            row = cursor.fetchone()

        assert row is not None
        return row

    def test_claims_encodes_and_marks_real_event_published(self) -> None:
        self._register_event()
        store = PostgresOutboxStore(self.pool)

        events = store.claim_pending(
            lease_id="worker-a:claim-001",
            now=_NOW,
            lease_expires_at=_NOW + timedelta(seconds=30),
            limit=10,
        )

        self.assertEqual(len(events), 1)
        event = events[0]
        payload = json.loads(event.payload)
        headers = dict(event.headers)

        self.assertEqual(event.attempt_count, 1)
        self.assertEqual(
            event.destination_topic,
            "factoryflow.batch.ingested.v1",
        )
        self.assertEqual(
            payload["event_type"],
            "factoryflow.batch.ingested",
        )
        self.assertEqual(
            headers["content_type"],
            b"application/json",
        )

        store.mark_published(
            event_id=event.event_id,
            lease_id=event.lease_id,
            published_at=_NOW + timedelta(seconds=1),
        )
        state = self._state()

        self.assertEqual(state["attempt_count"], 1)
        self.assertIsNone(state["lease_owner"])
        self.assertIsNotNone(state["published_at"])

    def test_active_lease_excludes_other_worker(self) -> None:
        self._register_event()
        store = PostgresOutboxStore(self.pool)

        first = store.claim_pending(
            lease_id="worker-a:active",
            now=_NOW,
            lease_expires_at=_NOW + timedelta(seconds=30),
            limit=10,
        )
        second = store.claim_pending(
            lease_id="worker-b:blocked",
            now=_NOW + timedelta(seconds=1),
            lease_expires_at=_NOW + timedelta(seconds=31),
            limit=10,
        )

        self.assertEqual(len(first), 1)
        self.assertEqual(second, ())
        self.assertEqual(self._state()["attempt_count"], 1)

    def test_expired_lease_is_reclaimed_and_stale_owner_is_fenced(
        self,
    ) -> None:
        self._register_event()
        store = PostgresOutboxStore(self.pool)

        first = store.claim_pending(
            lease_id="worker-a:expired",
            now=_NOW,
            lease_expires_at=_NOW + timedelta(seconds=1),
            limit=10,
        )[0]
        second = store.claim_pending(
            lease_id="worker-b:replacement",
            now=_NOW + timedelta(seconds=2),
            lease_expires_at=_NOW + timedelta(seconds=32),
            limit=10,
        )[0]

        self.assertEqual(second.attempt_count, 2)

        with self.assertRaises(OutboxLeaseLostError):
            store.mark_published(
                event_id=first.event_id,
                lease_id=first.lease_id,
                published_at=_NOW + timedelta(seconds=3),
            )

        store.mark_published(
            event_id=second.event_id,
            lease_id=second.lease_id,
            published_at=_NOW + timedelta(seconds=3),
        )
        self.assertIsNotNone(self._state()["published_at"])

    def test_concurrent_workers_claim_event_only_once(self) -> None:
        self._register_event()

        def claim(lease_id: str) -> tuple[UUID, ...]:
            events = PostgresOutboxStore(self.pool).claim_pending(
                lease_id=lease_id,
                now=_NOW,
                lease_expires_at=_NOW + timedelta(seconds=30),
                limit=1,
            )
            return tuple(event.event_id for event in events)

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = tuple(
                executor.map(
                    claim,
                    ("worker-a:race", "worker-b:race"),
                )
            )

        claimed_ids = tuple(event_id for result in results for event_id in result)
        self.assertEqual(len(claimed_ids), 1)
        self.assertEqual(self._state()["attempt_count"], 1)

    def test_reschedule_delays_reclaim_and_increments_attempt(self) -> None:
        self._register_event()
        store = PostgresOutboxStore(self.pool)

        first = store.claim_pending(
            lease_id="worker-a:retry",
            now=_NOW,
            lease_expires_at=_NOW + timedelta(seconds=30),
            limit=1,
        )[0]
        retry_at = _NOW + timedelta(seconds=10)

        store.reschedule(
            event_id=first.event_id,
            lease_id=first.lease_id,
            available_at=retry_at,
            error_code="publisher_retryable",
        )

        self.assertEqual(
            store.claim_pending(
                lease_id="worker-b:too-early",
                now=retry_at - timedelta(microseconds=1),
                lease_expires_at=retry_at + timedelta(seconds=30),
                limit=1,
            ),
            (),
        )

        second = store.claim_pending(
            lease_id="worker-b:retry",
            now=retry_at,
            lease_expires_at=retry_at + timedelta(seconds=30),
            limit=1,
        )[0]

        self.assertEqual(second.event_id, first.event_id)
        self.assertEqual(second.attempt_count, 2)
        self.assertEqual(self._state()["last_error"], "publisher_retryable")

    def test_dead_lettered_event_cannot_be_claimed_again(self) -> None:
        self._register_event()
        store = PostgresOutboxStore(self.pool)

        event = store.claim_pending(
            lease_id="worker-a:terminal",
            now=_NOW,
            lease_expires_at=_NOW + timedelta(seconds=30),
            limit=1,
        )[0]

        store.mark_dead_lettered(
            event_id=event.event_id,
            lease_id=event.lease_id,
            dead_lettered_at=_NOW + timedelta(seconds=1),
            error_code="attempt_limit_reached",
        )

        claimed_again = store.claim_pending(
            lease_id="worker-b:terminal",
            now=_NOW + timedelta(days=1),
            lease_expires_at=_NOW + timedelta(days=1, seconds=30),
            limit=1,
        )
        state = self._state()

        self.assertEqual(claimed_again, ())
        self.assertIsNotNone(state["dead_lettered_at"])
        self.assertIsNone(state["lease_owner"])
        self.assertEqual(state["last_error"], "attempt_limit_reached")
