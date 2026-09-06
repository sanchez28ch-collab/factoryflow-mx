"""End-to-end integration test for PostgreSQL-to-Kafka outbox delivery."""

import json
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from time import monotonic
from unittest import TestCase, skipUnless
from uuid import UUID, uuid4

import psycopg
from confluent_kafka import Consumer, Message, TopicPartition
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from factoryflow_batch.adapters.kafka_event_publisher import (
    ConfluentKafkaEventPublisher,
    KafkaPublisherConfiguration,
)
from factoryflow_batch.adapters.postgres_batch_registry import (
    PostgresBatchRegistry,
)
from factoryflow_batch.adapters.postgres_outbox_store import (
    PostgresOutboxStore,
)
from factoryflow_batch.application.outbox_models import (
    OutboxPublicationPolicy,
)
from factoryflow_batch.application.publish_outbox import (
    OutboxPublisherService,
)
from factoryflow_batch.domain import BatchManifest
from factoryflow_batch.ports import (
    LeasedOutboxEvent,
    PublicationReceipt,
)

_DATABASE_URL = os.environ.get("P01_TEST_DATABASE_URL")
_KAFKA_BOOTSTRAP_SERVERS = os.environ.get("P01_TEST_KAFKA_BOOTSTRAP_SERVERS")


@dataclass(frozen=True, slots=True)
class _FixedClock:
    value: datetime

    def now(self) -> datetime:
        return self.value


@dataclass(frozen=True, slots=True)
class _FixedLeaseIdGenerator:
    value: str

    def new_lease_id(self) -> str:
        return self.value


class _RecordingPublisher:
    def __init__(
        self,
        delegate: ConfluentKafkaEventPublisher,
    ) -> None:
        self._delegate = delegate
        self.receipt: PublicationReceipt | None = None

    def publish(
        self,
        event: LeasedOutboxEvent,
        /,
    ) -> PublicationReceipt:
        receipt = self._delegate.publish(event)
        self.receipt = receipt
        return receipt


@skipUnless(
    _DATABASE_URL is not None and _KAFKA_BOOTSTRAP_SERVERS is not None,
    "PostgreSQL and Kafka integration settings are required.",
)
class TestPostgresKafkaOutboxPipeline(TestCase):
    """Verify durable broker-confirmed outbox publication."""

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
            max_size=4,
            timeout=10,
            kwargs={
                "autocommit": True,
                "application_name": "factoryflow-p01-e2e-tests",
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

    def _register_event(
        self,
        *,
        now: datetime,
    ) -> dict[str, object]:
        batch_id = uuid4()
        manifest = BatchManifest.create(
            batch_id=batch_id,
            source_system="erp",
            dataset="kafka_pipeline_orders",
            schema_version="1.0.0",
            logical_date=date(2026, 9, 5),
            source_file_name=f"{batch_id}.csv",
            landing_uri=f"file:///landing/{batch_id}.csv",
            content_type="text/csv",
            extracted_at=now - timedelta(minutes=2),
            landed_at=now - timedelta(minutes=1),
            record_count=3,
            file_size_bytes=128,
            sha256="d" * 64,
        )
        PostgresBatchRegistry(self.pool).register_if_absent(manifest)

        with (
            self.pool.connection() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                UPDATE ingestion.outbox_events
                SET available_at = %s
                WHERE aggregate_id = %s
                RETURNING
                    event_id,
                    destination_topic,
                    partition_key,
                    payload,
                    headers
                """,
                (now, batch_id),
            )
            row = cursor.fetchone()

        assert row is not None
        return row

    def test_delivers_and_commits_outbox_event(self) -> None:
        assert _KAFKA_BOOTSTRAP_SERVERS is not None

        now = datetime.now(UTC)
        expected = self._register_event(now=now)
        event_id = expected["event_id"]
        assert isinstance(event_id, UUID)

        recording_publisher = _RecordingPublisher(
            ConfluentKafkaEventPublisher(
                KafkaPublisherConfiguration(
                    bootstrap_servers=_KAFKA_BOOTSTRAP_SERVERS,
                    client_id=f"factoryflow-p01-e2e-{event_id}",
                    delivery_timeout_ms=15_000,
                    request_timeout_ms=10_000,
                    poll_interval_ms=25,
                )
            )
        )
        service = OutboxPublisherService(
            store=PostgresOutboxStore(self.pool),
            publisher=recording_publisher,
            clock=_FixedClock(now),
            lease_ids=_FixedLeaseIdGenerator(f"e2e:{uuid4()}"),
            policy=OutboxPublicationPolicy(
                batch_size=1,
                lease_duration=timedelta(seconds=30),
            ),
        )

        report = service.run_once()

        self.assertEqual(report.claimed, 1)
        self.assertEqual(report.published, 1)
        self.assertEqual(report.rescheduled, 0)
        self.assertEqual(report.dead_lettered, 0)

        receipt = recording_publisher.receipt
        self.assertIsNotNone(receipt)
        assert receipt is not None

        with (
            self.pool.connection() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT
                    attempt_count,
                    published_at,
                    dead_lettered_at,
                    lease_owner,
                    payload,
                    headers
                FROM ingestion.outbox_events
                WHERE event_id = %s
                """,
                (event_id,),
            )
            state = cursor.fetchone()

        assert state is not None
        self.assertEqual(state["attempt_count"], 1)
        self.assertIsNotNone(state["published_at"])
        self.assertIsNone(state["dead_lettered_at"])
        self.assertIsNone(state["lease_owner"])

        received = self._consume_receipt(receipt)
        self.assertEqual(received.key(), str(expected["partition_key"]).encode())
        self.assertEqual(
            json.loads(received.value()),
            state["payload"],
        )

        headers = dict(received.headers() or ())
        self.assertEqual(
            headers["event_id"],
            str(event_id).encode("ascii"),
        )
        self.assertEqual(
            headers["content_type"],
            b"application/json",
        )

    def _consume_receipt(
        self,
        receipt: PublicationReceipt,
    ) -> Message:
        assert _KAFKA_BOOTSTRAP_SERVERS is not None

        consumer = Consumer(
            {
                "bootstrap.servers": _KAFKA_BOOTSTRAP_SERVERS,
                "group.id": f"factoryflow-p01-e2e-{uuid4()}",
                "security.protocol": "PLAINTEXT",
                "enable.auto.commit": False,
                "allow.auto.create.topics": False,
            }
        )

        try:
            consumer.assign(
                [
                    TopicPartition(
                        receipt.topic,
                        receipt.partition,
                        receipt.offset,
                    )
                ]
            )
            deadline = monotonic() + 15

            while monotonic() < deadline:
                message = consumer.poll(1.0)

                if message is None:
                    continue

                error = message.error()
                if error is not None:
                    self.fail(f"Kafka consumer error code: {error.code()}")

                return message
        finally:
            consumer.close()

        self.fail("The confirmed Kafka message was not consumed.")
