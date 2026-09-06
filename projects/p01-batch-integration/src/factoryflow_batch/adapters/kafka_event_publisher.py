"""Idempotent Kafka producer adapter with confirmed broker delivery."""

from __future__ import annotations

import re
from dataclasses import dataclass
from threading import Event
from time import monotonic

from confluent_kafka import (
    KafkaError,
    KafkaException,
    Message,
    Producer,
)

from factoryflow_batch.adapters.exceptions import (
    KafkaPublisherConfigurationError,
)
from factoryflow_batch.ports import (
    EventPublicationError,
    LeasedOutboxEvent,
    PublicationReceipt,
)

_CLIENT_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SUPPORTED_COMPRESSION = frozenset({"gzip", "lz4", "snappy", "zstd"})
_MAX_PAYLOAD_BYTES = 1_000_000
_MAX_HEADER_BYTES = 8_192


@dataclass(frozen=True, slots=True)
class KafkaPublisherConfiguration:
    """Validated settings for one idempotent Kafka producer."""

    bootstrap_servers: str
    client_id: str = "factoryflow-p01-outbox"
    delivery_timeout_ms: int = 30_000
    request_timeout_ms: int = 10_000
    poll_interval_ms: int = 50
    compression_type: str = "zstd"

    def __post_init__(self) -> None:
        """Reject unsafe endpoints and unreasonable timeout values."""

        if (
            not isinstance(self.bootstrap_servers, str)
            or not self.bootstrap_servers
            or len(self.bootstrap_servers) > 2_048
            or any(character.isspace() for character in self.bootstrap_servers)
            or any(marker in self.bootstrap_servers for marker in ("@", "/", "\\"))
        ):
            raise KafkaPublisherConfigurationError(
                "bootstrap_servers must contain broker host and port entries."
            )

        if not isinstance(self.client_id, str) or not _CLIENT_ID_PATTERN.fullmatch(self.client_id):
            raise KafkaPublisherConfigurationError("client_id uses an unsupported format.")

        _validate_integer(
            name="delivery_timeout_ms",
            value=self.delivery_timeout_ms,
            minimum=1_000,
            maximum=300_000,
        )
        _validate_integer(
            name="request_timeout_ms",
            value=self.request_timeout_ms,
            minimum=500,
            maximum=120_000,
        )
        _validate_integer(
            name="poll_interval_ms",
            value=self.poll_interval_ms,
            minimum=1,
            maximum=1_000,
        )

        if self.request_timeout_ms > self.delivery_timeout_ms:
            raise KafkaPublisherConfigurationError(
                "request_timeout_ms must not exceed delivery_timeout_ms."
            )

        if self.compression_type not in _SUPPORTED_COMPRESSION:
            raise KafkaPublisherConfigurationError("compression_type is not supported.")

    def producer_settings(self) -> dict[str, object]:
        """Return hardened librdkafka producer settings."""

        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "security.protocol": "PLAINTEXT",
            "enable.idempotence": True,
            "acks": "all",
            "retries": 2_147_483_647,
            "max.in.flight.requests.per.connection": 5,
            "delivery.timeout.ms": self.delivery_timeout_ms,
            "request.timeout.ms": self.request_timeout_ms,
            "socket.timeout.ms": self.request_timeout_ms,
            "compression.type": self.compression_type,
            "allow.auto.create.topics": False,
            "linger.ms": 5,
        }


class ConfluentKafkaEventPublisher:
    """Publish one outbox event and await its broker acknowledgement."""

    def __init__(
        self,
        configuration: KafkaPublisherConfiguration,
        *,
        producer: Producer | None = None,
    ) -> None:
        self._configuration = configuration

        if producer is not None:
            self._producer = producer
            return

        try:
            self._producer = Producer(configuration.producer_settings())
        except (KafkaException, ValueError) as exc:
            raise KafkaPublisherConfigurationError(
                "Kafka producer could not be configured."
            ) from exc

    def publish(
        self,
        event: LeasedOutboxEvent,
        /,
    ) -> PublicationReceipt:
        """Publish and return coordinates only after broker confirmation."""

        _validate_event(event)

        completed = Event()
        delivery_error: KafkaError | None = None
        delivered_message: Message | None = None

        def on_delivery(
            error: KafkaError | None,
            message: Message,
        ) -> None:
            nonlocal delivery_error, delivered_message

            delivery_error = error
            delivered_message = message
            completed.set()

        try:
            self._producer.produce(
                topic=event.destination_topic,
                key=event.partition_key.encode("utf-8"),
                value=event.payload,
                headers=_publication_headers(event),
                timestamp=int(event.occurred_at.timestamp() * 1_000),
                on_delivery=on_delivery,
            )
            self._await_delivery(completed)
        except BufferError as exc:
            raise EventPublicationError(retryable=True) from exc
        except KafkaException as exc:
            raise EventPublicationError(retryable=_is_retryable_exception(exc)) from exc
        except (TypeError, ValueError) as exc:
            raise EventPublicationError(retryable=False) from exc

        if delivery_error is not None:
            raise EventPublicationError(
                retryable=(bool(delivery_error.retriable()) and not bool(delivery_error.fatal()))
            )

        if delivered_message is None:
            raise EventPublicationError(retryable=True)

        return _receipt(
            event=event,
            message=delivered_message,
        )

    def _await_delivery(self, completed: Event) -> None:
        deadline = monotonic() + (self._configuration.delivery_timeout_ms / 1_000)
        poll_interval = self._configuration.poll_interval_ms / 1_000

        while not completed.is_set():
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise EventPublicationError(retryable=True)

            self._producer.poll(min(poll_interval, remaining))


def _receipt(
    *,
    event: LeasedOutboxEvent,
    message: Message,
) -> PublicationReceipt:
    topic = message.topic()
    partition = message.partition()
    offset = message.offset()

    if (
        topic != event.destination_topic
        or isinstance(partition, bool)
        or not isinstance(partition, int)
        or partition < 0
        or isinstance(offset, bool)
        or not isinstance(offset, int)
        or offset < 0
    ):
        raise EventPublicationError(retryable=False)

    return PublicationReceipt(
        topic=topic,
        partition=partition,
        offset=offset,
    )


def _publication_headers(
    event: LeasedOutboxEvent,
) -> list[tuple[str, str | bytes | None]]:
    headers: dict[str, str | bytes | None] = dict(event.headers)
    headers["event_id"] = str(event.event_id).encode("ascii")
    return sorted(headers.items())


def _validate_event(event: LeasedOutboxEvent) -> None:
    if (
        not event.destination_topic
        or not event.partition_key
        or not event.payload
        or len(event.payload) > _MAX_PAYLOAD_BYTES
    ):
        raise EventPublicationError(retryable=False)

    for name, value in event.headers:
        if (
            not isinstance(name, str)
            or not name
            or not isinstance(value, bytes)
            or len(value) > _MAX_HEADER_BYTES
        ):
            raise EventPublicationError(retryable=False)


def _is_retryable_exception(exception: KafkaException) -> bool:
    if not exception.args:
        return True

    error = exception.args[0]
    if not isinstance(error, KafkaError):
        return True

    return bool(error.retriable()) and not bool(error.fatal())


def _validate_integer(
    *,
    name: str,
    value: int,
    minimum: int,
    maximum: int,
) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise KafkaPublisherConfigurationError(f"{name} must be between {minimum} and {maximum}.")
