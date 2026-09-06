"""Unit tests for hardened Kafka publisher configuration."""

from unittest import TestCase

from factoryflow_batch.adapters.exceptions import (
    KafkaPublisherConfigurationError,
)
from factoryflow_batch.adapters.kafka_event_publisher import (
    KafkaPublisherConfiguration,
)


class TestKafkaPublisherConfiguration(TestCase):
    """Verify safe and deterministic producer settings."""

    def test_builds_hardened_idempotent_settings(self) -> None:
        settings = KafkaPublisherConfiguration(
            bootstrap_servers="localhost:9092",
            client_id="factoryflow-p01-test",
        ).producer_settings()

        self.assertEqual(
            settings["bootstrap.servers"],
            "localhost:9092",
        )
        self.assertEqual(
            settings["client.id"],
            "factoryflow-p01-test",
        )
        self.assertEqual(settings["security.protocol"], "PLAINTEXT")
        self.assertIs(settings["enable.idempotence"], True)
        self.assertEqual(settings["acks"], "all")
        self.assertEqual(settings["retries"], 2_147_483_647)
        self.assertEqual(
            settings["max.in.flight.requests.per.connection"],
            5,
        )
        self.assertIs(settings["allow.auto.create.topics"], False)
        self.assertEqual(settings["compression.type"], "zstd")

    def test_rejects_unsafe_bootstrap_servers(self) -> None:
        invalid_values = (
            "",
            "localhost:9092 broker:9092",
            "user@localhost:9092",
            "http://localhost:9092",
            "host\\broker:9092",
            "x" * 2_049,
        )

        for value in invalid_values:
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(
                    KafkaPublisherConfigurationError,
                    "bootstrap_servers",
                ),
            ):
                KafkaPublisherConfiguration(
                    bootstrap_servers=value,
                )

    def test_rejects_invalid_client_identifiers(self) -> None:
        invalid_values = (
            "",
            "-worker",
            "worker with spaces",
            "worker/unsafe",
            "x" * 129,
        )

        for value in invalid_values:
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(
                    KafkaPublisherConfigurationError,
                    "client_id",
                ),
            ):
                KafkaPublisherConfiguration(
                    bootstrap_servers="localhost:9092",
                    client_id=value,
                )

    def test_rejects_timeout_and_poll_values_outside_limits(
        self,
    ) -> None:
        invalid_options = (
            {"delivery_timeout_ms": 999},
            {"delivery_timeout_ms": 300_001},
            {"delivery_timeout_ms": True},
            {"request_timeout_ms": 499},
            {"request_timeout_ms": 120_001},
            {"request_timeout_ms": True},
            {"poll_interval_ms": 0},
            {"poll_interval_ms": 1_001},
            {"poll_interval_ms": True},
        )

        for options in invalid_options:
            with (
                self.subTest(options=options),
                self.assertRaises(
                    KafkaPublisherConfigurationError,
                ),
            ):
                KafkaPublisherConfiguration(
                    bootstrap_servers="localhost:9092",
                    **options,
                )

    def test_rejects_request_timeout_above_delivery_timeout(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            KafkaPublisherConfigurationError,
            "must not exceed",
        ):
            KafkaPublisherConfiguration(
                bootstrap_servers="localhost:9092",
                delivery_timeout_ms=1_000,
                request_timeout_ms=1_001,
            )

    def test_accepts_supported_compression_algorithms(self) -> None:
        for compression in ("gzip", "lz4", "snappy", "zstd"):
            with self.subTest(compression=compression):
                configuration = KafkaPublisherConfiguration(
                    bootstrap_servers="localhost:9092",
                    compression_type=compression,
                )

                self.assertEqual(
                    configuration.producer_settings()["compression.type"],
                    compression,
                )

    def test_rejects_unsupported_compression(self) -> None:
        with self.assertRaisesRegex(
            KafkaPublisherConfigurationError,
            "compression_type",
        ):
            KafkaPublisherConfiguration(
                bootstrap_servers="localhost:9092",
                compression_type="none",
            )
