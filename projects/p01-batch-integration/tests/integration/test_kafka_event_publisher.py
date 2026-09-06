"""Integration tests for confirmed publication against real Kafka."""

import json
import os
from datetime import UTC, datetime
from time import monotonic
from unittest import TestCase, skipUnless
from uuid import uuid4

from confluent_kafka import Consumer, Message, TopicPartition

from factoryflow_batch.adapters.kafka_event_publisher import (
    ConfluentKafkaEventPublisher,
    KafkaPublisherConfiguration,
)
from factoryflow_batch.ports import LeasedOutboxEvent

_KAFKA_BOOTSTRAP_SERVERS = os.environ.get("P01_TEST_KAFKA_BOOTSTRAP_SERVERS")
_TOPIC = "factoryflow.batch.ingested.v1"


@skipUnless(
    _KAFKA_BOOTSTRAP_SERVERS,
    "P01_TEST_KAFKA_BOOTSTRAP_SERVERS is not configured.",
)
class TestKafkaEventPublisherIntegration(TestCase):
    """Verify broker-confirmed messages can be consumed intact."""

    def test_publishes_and_consumes_confirmed_event(self) -> None:
        assert _KAFKA_BOOTSTRAP_SERVERS is not None

        event_id = uuid4()
        occurred_at = datetime.now(UTC)
        partition_key = f"batch-{uuid4()}"
        payload = json.dumps(
            {
                "event_id": str(event_id),
                "event_type": "batch.ingested",
                "event_version": "1.0.0",
                "aggregate_id": partition_key,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

        event = LeasedOutboxEvent(
            event_id=event_id,
            destination_topic=_TOPIC,
            partition_key=partition_key,
            payload=payload,
            headers=(
                ("content-type", b"application/json"),
                (
                    "event-schema",
                    b"urn:factoryflow:events:batch-ingested:1.0.0",
                ),
            ),
            occurred_at=occurred_at,
            attempt_count=1,
            lease_id=f"integration:{uuid4()}",
        )

        publisher = ConfluentKafkaEventPublisher(
            KafkaPublisherConfiguration(
                bootstrap_servers=_KAFKA_BOOTSTRAP_SERVERS,
                client_id=f"factoryflow-p01-integration-{event_id}",
                delivery_timeout_ms=15_000,
                request_timeout_ms=10_000,
                poll_interval_ms=25,
            )
        )

        receipt = publisher.publish(event)

        self.assertEqual(receipt.topic, _TOPIC)
        self.assertGreaterEqual(receipt.partition, 0)
        self.assertGreaterEqual(receipt.offset, 0)

        consumer = Consumer(
            {
                "bootstrap.servers": _KAFKA_BOOTSTRAP_SERVERS,
                "group.id": f"factoryflow-p01-integration-{uuid4()}",
                "security.protocol": "PLAINTEXT",
                "enable.auto.commit": False,
                "allow.auto.create.topics": False,
                "auto.offset.reset": "earliest",
            }
        )
        received: Message | None = None

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

                received = message
                break
        finally:
            consumer.close()

        self.assertIsNotNone(received)
        assert received is not None

        self.assertEqual(received.topic(), receipt.topic)
        self.assertEqual(received.partition(), receipt.partition)
        self.assertEqual(received.offset(), receipt.offset)
        self.assertEqual(received.key(), partition_key.encode("utf-8"))
        self.assertEqual(received.value(), payload)

        headers = dict(received.headers() or ())
        self.assertEqual(
            headers["event_id"],
            str(event_id).encode("ascii"),
        )
        self.assertEqual(
            headers["content-type"],
            b"application/json",
        )
        self.assertEqual(
            headers["event-schema"],
            b"urn:factoryflow:events:batch-ingested:1.0.0",
        )
