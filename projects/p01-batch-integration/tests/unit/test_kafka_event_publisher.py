"""Unit tests for confirmed Kafka event publication."""

from dataclasses import replace
from datetime import UTC, datetime
from unittest import TestCase
from unittest.mock import MagicMock, patch
from uuid import UUID

import factoryflow_batch.adapters.kafka_event_publisher as publisher_module
from factoryflow_batch.adapters.exceptions import (
    KafkaPublisherConfigurationError,
)
from factoryflow_batch.adapters.kafka_event_publisher import (
    ConfluentKafkaEventPublisher,
    KafkaPublisherConfiguration,
)
from factoryflow_batch.ports import (
    EventPublicationError,
    LeasedOutboxEvent,
)

_NOW = datetime(2026, 9, 5, 20, 30, tzinfo=UTC)
_EVENT_ID = UUID("17900b46-b816-465b-b8e8-bb41bb411966")
_TOPIC = "factoryflow.batch.ingested.v1"


def _event() -> LeasedOutboxEvent:
    return LeasedOutboxEvent(
        event_id=_EVENT_ID,
        destination_topic=_TOPIC,
        partition_key="batch-001",
        payload=b'{"event_id":"17900b46-b816-465b-b8e8-bb41bb411966"}',
        headers=(
            ("content-type", b"application/json"),
            ("event-schema", b"batch-ingested-v1"),
        ),
        occurred_at=_NOW,
        attempt_count=1,
        lease_id="worker-01:kafka-test",
    )


def _configuration() -> KafkaPublisherConfiguration:
    return KafkaPublisherConfiguration(
        bootstrap_servers="localhost:9092",
        client_id="factoryflow-p01-unit-test",
        delivery_timeout_ms=1_000,
        request_timeout_ms=500,
        poll_interval_ms=1,
    )


def _message(
    *,
    topic: str = _TOPIC,
    partition: int = 2,
    offset: int = 41,
) -> MagicMock:
    message = MagicMock()
    message.topic.return_value = topic
    message.partition.return_value = partition
    message.offset.return_value = offset
    return message


def _producer_delivering(
    *,
    message: object,
    error: object | None = None,
) -> MagicMock:
    producer = MagicMock()

    def poll(_: float) -> int:
        callback = producer.produce.call_args.kwargs["on_delivery"]
        callback(error, message)
        return 1

    producer.poll.side_effect = poll
    return producer


class TestConfluentKafkaEventPublisher(TestCase):
    """Verify broker-confirmed publication and controlled failures."""

    def test_publishes_payload_key_headers_and_timestamp(self) -> None:
        message = _message()
        producer = _producer_delivering(message=message)
        publisher = ConfluentKafkaEventPublisher(
            _configuration(),
            producer=producer,
        )

        receipt = publisher.publish(_event())

        self.assertEqual(receipt.topic, _TOPIC)
        self.assertEqual(receipt.partition, 2)
        self.assertEqual(receipt.offset, 41)

        arguments = producer.produce.call_args.kwargs
        self.assertEqual(arguments["topic"], _TOPIC)
        self.assertEqual(arguments["key"], b"batch-001")
        self.assertEqual(arguments["value"], _event().payload)
        self.assertEqual(
            arguments["timestamp"],
            int(_NOW.timestamp() * 1_000),
        )

        headers = dict(arguments["headers"])
        self.assertEqual(
            headers["event_id"],
            str(_EVENT_ID).encode("ascii"),
        )
        self.assertEqual(
            headers["content-type"],
            b"application/json",
        )
        producer.poll.assert_called()

    def test_maps_full_producer_queue_to_retryable_failure(
        self,
    ) -> None:
        producer = MagicMock()
        producer.produce.side_effect = BufferError(
            "internal queue detail",
        )
        publisher = ConfluentKafkaEventPublisher(
            _configuration(),
            producer=producer,
        )

        with self.assertRaises(EventPublicationError) as raised:
            publisher.publish(_event())

        self.assertTrue(raised.exception.retryable)
        self.assertNotIn(
            "internal queue detail",
            str(raised.exception),
        )

    def test_times_out_without_unconfirmed_success(self) -> None:
        producer = MagicMock()
        publisher = ConfluentKafkaEventPublisher(
            _configuration(),
            producer=producer,
        )

        with (
            patch.object(
                publisher_module,
                "monotonic",
                side_effect=(0.0, 2.0),
            ),
            self.assertRaises(EventPublicationError) as raised,
        ):
            publisher.publish(_event())

        self.assertTrue(raised.exception.retryable)
        producer.poll.assert_not_called()

    def test_classifies_delivery_callback_errors(self) -> None:
        cases = (
            (True, False, True),
            (False, False, False),
            (True, True, False),
        )

        for retriable, fatal, expected_retryable in cases:
            error = MagicMock()
            error.retriable.return_value = retriable
            error.fatal.return_value = fatal
            producer = _producer_delivering(
                message=_message(),
                error=error,
            )
            publisher = ConfluentKafkaEventPublisher(
                _configuration(),
                producer=producer,
            )

            with (
                self.subTest(
                    retriable=retriable,
                    fatal=fatal,
                ),
                self.assertRaises(EventPublicationError) as raised,
            ):
                publisher.publish(_event())

            self.assertEqual(
                raised.exception.retryable,
                expected_retryable,
            )

    def test_rejects_invalid_broker_coordinates(self) -> None:
        cases = (
            _message(topic="unexpected.topic"),
            _message(partition=-1),
            _message(offset=-1),
        )

        for message in cases:
            producer = _producer_delivering(message=message)
            publisher = ConfluentKafkaEventPublisher(
                _configuration(),
                producer=producer,
            )

            with (
                self.subTest(message=message),
                self.assertRaises(EventPublicationError) as raised,
            ):
                publisher.publish(_event())

            self.assertFalse(raised.exception.retryable)

    def test_rejects_invalid_events_before_production(self) -> None:
        invalid_events = (
            replace(_event(), destination_topic=""),
            replace(_event(), partition_key=""),
            replace(_event(), payload=b""),
            replace(_event(), payload=b"x" * 1_000_001),
            replace(_event(), headers=(("", b"value"),)),
            replace(
                _event(),
                headers=(("oversized", b"x" * 8_193),),
            ),
        )

        for event in invalid_events:
            producer = MagicMock()
            publisher = ConfluentKafkaEventPublisher(
                _configuration(),
                producer=producer,
            )

            with (
                self.subTest(event=event),
                self.assertRaises(EventPublicationError) as raised,
            ):
                publisher.publish(event)

            self.assertFalse(raised.exception.retryable)
            producer.produce.assert_not_called()

    def test_redacts_producer_configuration_failure(self) -> None:
        with (
            patch.object(
                publisher_module,
                "Producer",
                side_effect=ValueError("credential-like detail"),
            ),
            self.assertRaises(
                KafkaPublisherConfigurationError,
            ) as raised,
        ):
            ConfluentKafkaEventPublisher(_configuration())

        self.assertNotIn(
            "credential-like detail",
            str(raised.exception),
        )
