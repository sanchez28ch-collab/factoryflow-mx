"""Unit tests for the operational outbox publisher command."""

import json
from io import StringIO
from unittest import TestCase
from unittest.mock import MagicMock, patch

from psycopg_pool import PoolTimeout

import factoryflow_batch.entrypoints.publish_outbox as command
from factoryflow_batch.application.exceptions import (
    OutboxPublicationConsistencyError,
    OutboxPublicationPersistenceError,
)
from factoryflow_batch.application.outbox_models import (
    OutboxPublicationReport,
)


def _environment() -> dict[str, str]:
    return {
        "P01_DATABASE_URL": (
            "postgresql://factoryflow:supersecret@localhost:5432/factoryflow_p01_test"
        ),
        "P01_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    }


class TestPublishOutboxCommand(TestCase):
    """Verify stable exits and non-sensitive structured output."""

    def test_rejects_missing_database_url_before_connecting(
        self,
    ) -> None:
        stdout = StringIO()
        stderr = StringIO()
        pool_factory = MagicMock()

        with patch.object(
            command,
            "ConnectionPool",
            pool_factory,
        ):
            exit_code = command.main(
                ["--run-once"],
                environment={"P01_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"},
                stdout=stdout,
                stderr=stderr,
            )

        self.assertEqual(
            exit_code,
            command.EXIT_CONFIGURATION,
        )
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(
            json.loads(stderr.getvalue())["error_code"],
            "configuration_error",
        )
        pool_factory.assert_not_called()

    def test_rejects_missing_kafka_endpoint_before_connecting(
        self,
    ) -> None:
        stdout = StringIO()
        stderr = StringIO()
        pool_factory = MagicMock()

        with patch.object(
            command,
            "ConnectionPool",
            pool_factory,
        ):
            exit_code = command.main(
                ["--run-once"],
                environment={"P01_DATABASE_URL": ("postgresql://user:secret@localhost/database")},
                stdout=stdout,
                stderr=stderr,
            )

        self.assertEqual(
            exit_code,
            command.EXIT_CONFIGURATION,
        )
        pool_factory.assert_not_called()
        self.assertNotIn("secret", stderr.getvalue())

    def test_rejects_unsafe_worker_identifier(self) -> None:
        stdout = StringIO()
        stderr = StringIO()
        pool_factory = MagicMock()

        with patch.object(
            command,
            "ConnectionPool",
            pool_factory,
        ):
            exit_code = command.main(
                [
                    "--run-once",
                    "--worker-id",
                    "unsafe worker identifier",
                ],
                environment=_environment(),
                stdout=stdout,
                stderr=stderr,
            )

        self.assertEqual(
            exit_code,
            command.EXIT_CONFIGURATION,
        )
        pool_factory.assert_not_called()

    def test_reports_successful_single_cycle(self) -> None:
        stdout = StringIO()
        stderr = StringIO()
        pool = MagicMock()
        service = MagicMock()
        service.run_once.return_value = OutboxPublicationReport(
            claimed=2,
            published=1,
            rescheduled=1,
            dead_lettered=0,
        )

        with (
            patch.object(
                command,
                "ConnectionPool",
                return_value=pool,
            ),
            patch.object(command, "PostgresOutboxStore"),
            patch.object(
                command,
                "ConfluentKafkaEventPublisher",
            ),
            patch.object(
                command,
                "OutboxPublisherService",
                return_value=service,
            ),
        ):
            exit_code = command.main(
                ["--run-once"],
                environment=_environment(),
                stdout=stdout,
                stderr=stderr,
            )

        self.assertEqual(exit_code, command.EXIT_SUCCESS)
        self.assertEqual(stderr.getvalue(), "")
        pool.open.assert_called_once_with(
            wait=True,
            timeout=10.0,
        )
        pool.close.assert_called_once_with(timeout=5.0)
        service.run_once.assert_called_once_with()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(
            payload["event"],
            "outbox_publication_cycle_completed",
        )
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["claimed"], 2)
        self.assertEqual(payload["published"], 1)
        self.assertEqual(payload["rescheduled"], 1)
        self.assertEqual(payload["dead_lettered"], 0)
        self.assertNotIn("supersecret", stdout.getvalue())

    def test_maps_pool_timeout_to_database_exit(self) -> None:
        stdout = StringIO()
        stderr = StringIO()
        pool = MagicMock()
        pool.open.side_effect = PoolTimeout("sensitive connection detail")

        with patch.object(
            command,
            "ConnectionPool",
            return_value=pool,
        ):
            exit_code = command.main(
                ["--run-once"],
                environment=_environment(),
                stdout=stdout,
                stderr=stderr,
            )

        self.assertEqual(exit_code, command.EXIT_DATABASE)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(
            json.loads(stderr.getvalue())["error_code"],
            "database_error",
        )
        self.assertNotIn(
            "sensitive connection detail",
            stderr.getvalue(),
        )
        self.assertNotIn("supersecret", stderr.getvalue())
        pool.close.assert_called_once_with(timeout=5.0)

    def test_maps_persistence_failure_to_database_exit(
        self,
    ) -> None:
        stdout = StringIO()
        stderr = StringIO()
        pool = MagicMock()
        service = MagicMock()
        service.run_once.side_effect = OutboxPublicationPersistenceError(
            "internal persistence detail"
        )

        with (
            patch.object(
                command,
                "ConnectionPool",
                return_value=pool,
            ),
            patch.object(command, "PostgresOutboxStore"),
            patch.object(
                command,
                "ConfluentKafkaEventPublisher",
            ),
            patch.object(
                command,
                "OutboxPublisherService",
                return_value=service,
            ),
        ):
            exit_code = command.main(
                ["--run-once"],
                environment=_environment(),
                stdout=stdout,
                stderr=stderr,
            )

        self.assertEqual(exit_code, command.EXIT_DATABASE)
        self.assertEqual(
            json.loads(stderr.getvalue())["error_code"],
            "database_error",
        )
        self.assertNotIn(
            "internal persistence detail",
            stderr.getvalue(),
        )

    def test_maps_consistency_failure_to_distinct_exit(
        self,
    ) -> None:
        stdout = StringIO()
        stderr = StringIO()
        pool = MagicMock()
        service = MagicMock()
        service.run_once.side_effect = OutboxPublicationConsistencyError(
            "internal consistency detail"
        )

        with (
            patch.object(
                command,
                "ConnectionPool",
                return_value=pool,
            ),
            patch.object(command, "PostgresOutboxStore"),
            patch.object(
                command,
                "ConfluentKafkaEventPublisher",
            ),
            patch.object(
                command,
                "OutboxPublisherService",
                return_value=service,
            ),
        ):
            exit_code = command.main(
                ["--run-once"],
                environment=_environment(),
                stdout=stdout,
                stderr=stderr,
            )

        self.assertEqual(
            exit_code,
            command.EXIT_CONSISTENCY,
        )
        self.assertEqual(
            json.loads(stderr.getvalue())["error_code"],
            "consistency_error",
        )
        self.assertNotIn(
            "internal consistency detail",
            stderr.getvalue(),
        )

    def test_continuous_mode_stops_gracefully_on_signal(
        self,
    ) -> None:
        output = StringIO()
        service = MagicMock()
        signal_mock = MagicMock(
            side_effect=(
                command.signal.SIG_DFL,
                command.signal.SIG_DFL,
                None,
                None,
            )
        )

        def run_once() -> OutboxPublicationReport:
            stop_handler = signal_mock.call_args_list[0].args[1]
            stop_handler(command.signal.SIGTERM, None)
            return OutboxPublicationReport(
                claimed=0,
                published=0,
                rescheduled=0,
                dead_lettered=0,
            )

        service.run_once.side_effect = run_once

        with patch.object(
            command.signal,
            "signal",
            signal_mock,
        ):
            exit_code = command._run_continuously(
                service=service,
                output=output,
                idle_wait_ms=1,
            )

        self.assertEqual(exit_code, command.EXIT_SUCCESS)
        service.run_once.assert_called_once_with()
        self.assertEqual(signal_mock.call_count, 4)

        events = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(
            [event["event"] for event in events],
            [
                "outbox_publication_cycle_completed",
                "outbox_publisher_stopped",
            ],
        )
        self.assertTrue(all(event["status"] == "ok" for event in events))
