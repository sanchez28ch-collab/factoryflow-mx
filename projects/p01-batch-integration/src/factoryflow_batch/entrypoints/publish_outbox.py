"""Operational command for publishing P01 transactional-outbox events."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from threading import Event
from types import FrameType
from typing import Any, Final, TextIO

import psycopg
from psycopg import Connection
from psycopg.conninfo import conninfo_to_dict
from psycopg_pool import ConnectionPool, PoolTimeout

from factoryflow_batch.adapters.exceptions import (
    KafkaPublisherConfigurationError,
    PostgresOutboxStoreError,
)
from factoryflow_batch.adapters.kafka_event_publisher import (
    ConfluentKafkaEventPublisher,
    KafkaPublisherConfiguration,
)
from factoryflow_batch.adapters.postgres_outbox_store import (
    PostgresOutboxStore,
)
from factoryflow_batch.adapters.runtime import (
    SystemUtcClock,
    UuidLeaseIdGenerator,
)
from factoryflow_batch.application.exceptions import (
    OutboxPublicationConfigurationError,
    OutboxPublicationConsistencyError,
    OutboxPublicationPersistenceError,
)
from factoryflow_batch.application.outbox_models import (
    OutboxPublicationPolicy,
    OutboxPublicationReport,
)
from factoryflow_batch.application.publish_outbox import (
    OutboxPublisherService,
)

EXIT_SUCCESS: Final = 0
EXIT_CONFIGURATION: Final = 2
EXIT_DATABASE: Final = 3
EXIT_CONSISTENCY: Final = 4

_DATABASE_URL_ENV: Final = "P01_DATABASE_URL"
_KAFKA_BOOTSTRAP_ENV: Final = "P01_KAFKA_BOOTSTRAP_SERVERS"
_WORKER_ID_ENV: Final = "P01_OUTBOX_WORKER_ID"
_BATCH_SIZE_ENV: Final = "P01_OUTBOX_BATCH_SIZE"
_LEASE_SECONDS_ENV: Final = "P01_OUTBOX_LEASE_SECONDS"
_MAX_ATTEMPTS_ENV: Final = "P01_OUTBOX_MAX_ATTEMPTS"
_RETRY_BASE_MS_ENV: Final = "P01_OUTBOX_RETRY_BASE_MS"
_RETRY_MAX_MS_ENV: Final = "P01_OUTBOX_RETRY_MAX_MS"
_IDLE_WAIT_MS_ENV: Final = "P01_OUTBOX_IDLE_WAIT_MS"
_CONNECT_TIMEOUT_ENV: Final = "P01_DATABASE_CONNECT_TIMEOUT_SECONDS"
_KAFKA_DELIVERY_TIMEOUT_ENV: Final = "P01_KAFKA_DELIVERY_TIMEOUT_MS"
_KAFKA_REQUEST_TIMEOUT_ENV: Final = "P01_KAFKA_REQUEST_TIMEOUT_MS"
_KAFKA_POLL_INTERVAL_ENV: Final = "P01_KAFKA_POLL_INTERVAL_MS"
_KAFKA_COMPRESSION_ENV: Final = "P01_KAFKA_COMPRESSION_TYPE"

_DEFAULT_WORKER_ID: Final = "factoryflow-p01-outbox"
_DEFAULT_BATCH_SIZE: Final = "100"
_DEFAULT_LEASE_SECONDS: Final = "30"
_DEFAULT_MAX_ATTEMPTS: Final = "10"
_DEFAULT_RETRY_BASE_MS: Final = "1000"
_DEFAULT_RETRY_MAX_MS: Final = "300000"
_DEFAULT_IDLE_WAIT_MS: Final = "1000"
_DEFAULT_CONNECT_TIMEOUT_SECONDS: Final = "10"
_DEFAULT_KAFKA_DELIVERY_TIMEOUT_MS: Final = "30000"
_DEFAULT_KAFKA_REQUEST_TIMEOUT_MS: Final = "10000"
_DEFAULT_KAFKA_POLL_INTERVAL_MS: Final = "50"
_DEFAULT_KAFKA_COMPRESSION: Final = "zstd"


class PublisherCommandConfigurationError(ValueError):
    """Raised when publisher process configuration is invalid."""


def _positive_integer(raw_value: str) -> int:
    try:
        value = int(raw_value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("the value must be an integer") from error

    if value <= 0:
        raise argparse.ArgumentTypeError("the value must be greater than zero")

    return value


def _build_parser(
    environment: Mapping[str, str],
) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="factoryflow-p01-publish-outbox",
        description=(
            "Publish P01 transactional-outbox events to Kafka. "
            "Database and Kafka endpoints are read only from environment."
        ),
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Process one bounded outbox batch and exit.",
    )
    parser.add_argument(
        "--worker-id",
        default=environment.get(
            _WORKER_ID_ENV,
            _DEFAULT_WORKER_ID,
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=_positive_integer,
        default=environment.get(
            _BATCH_SIZE_ENV,
            _DEFAULT_BATCH_SIZE,
        ),
    )
    parser.add_argument(
        "--lease-seconds",
        type=_positive_integer,
        default=environment.get(
            _LEASE_SECONDS_ENV,
            _DEFAULT_LEASE_SECONDS,
        ),
    )
    parser.add_argument(
        "--max-attempts",
        type=_positive_integer,
        default=environment.get(
            _MAX_ATTEMPTS_ENV,
            _DEFAULT_MAX_ATTEMPTS,
        ),
    )
    parser.add_argument(
        "--retry-base-ms",
        type=_positive_integer,
        default=environment.get(
            _RETRY_BASE_MS_ENV,
            _DEFAULT_RETRY_BASE_MS,
        ),
    )
    parser.add_argument(
        "--retry-max-ms",
        type=_positive_integer,
        default=environment.get(
            _RETRY_MAX_MS_ENV,
            _DEFAULT_RETRY_MAX_MS,
        ),
    )
    parser.add_argument(
        "--idle-wait-ms",
        type=_positive_integer,
        default=environment.get(
            _IDLE_WAIT_MS_ENV,
            _DEFAULT_IDLE_WAIT_MS,
        ),
    )
    parser.add_argument(
        "--connect-timeout-seconds",
        type=_positive_integer,
        default=environment.get(
            _CONNECT_TIMEOUT_ENV,
            _DEFAULT_CONNECT_TIMEOUT_SECONDS,
        ),
    )
    parser.add_argument(
        "--kafka-delivery-timeout-ms",
        type=_positive_integer,
        default=environment.get(
            _KAFKA_DELIVERY_TIMEOUT_ENV,
            _DEFAULT_KAFKA_DELIVERY_TIMEOUT_MS,
        ),
    )
    parser.add_argument(
        "--kafka-request-timeout-ms",
        type=_positive_integer,
        default=environment.get(
            _KAFKA_REQUEST_TIMEOUT_ENV,
            _DEFAULT_KAFKA_REQUEST_TIMEOUT_MS,
        ),
    )
    parser.add_argument(
        "--kafka-poll-interval-ms",
        type=_positive_integer,
        default=environment.get(
            _KAFKA_POLL_INTERVAL_ENV,
            _DEFAULT_KAFKA_POLL_INTERVAL_MS,
        ),
    )
    parser.add_argument(
        "--kafka-compression",
        default=environment.get(
            _KAFKA_COMPRESSION_ENV,
            _DEFAULT_KAFKA_COMPRESSION,
        ),
    )
    return parser


def _required_environment(
    environment: Mapping[str, str],
    name: str,
) -> str:
    value = environment.get(name)

    if value is None or not value.strip():
        raise PublisherCommandConfigurationError(f"{name} is required.")

    return value


def _database_url(environment: Mapping[str, str]) -> str:
    database_url = _required_environment(
        environment,
        _DATABASE_URL_ENV,
    )

    try:
        conninfo_to_dict(database_url)
    except psycopg.ProgrammingError as error:
        raise PublisherCommandConfigurationError(f"{_DATABASE_URL_ENV} is invalid.") from error

    return database_url


def _validate_process_limits(
    *,
    connect_timeout_seconds: int,
    idle_wait_ms: int,
) -> None:
    if connect_timeout_seconds > 300:
        raise PublisherCommandConfigurationError("connect timeout cannot exceed 300 seconds.")

    if idle_wait_ms > 60_000:
        raise PublisherCommandConfigurationError("idle wait cannot exceed 60000 milliseconds.")


def _policy(arguments: argparse.Namespace) -> OutboxPublicationPolicy:
    return OutboxPublicationPolicy(
        batch_size=arguments.batch_size,
        lease_duration=timedelta(seconds=arguments.lease_seconds),
        max_attempts=arguments.max_attempts,
        retry_base_delay=timedelta(milliseconds=arguments.retry_base_ms),
        retry_max_delay=timedelta(milliseconds=arguments.retry_max_ms),
    )


def _kafka_configuration(
    *,
    environment: Mapping[str, str],
    arguments: argparse.Namespace,
) -> KafkaPublisherConfiguration:
    return KafkaPublisherConfiguration(
        bootstrap_servers=_required_environment(
            environment,
            _KAFKA_BOOTSTRAP_ENV,
        ),
        client_id=arguments.worker_id,
        delivery_timeout_ms=(arguments.kafka_delivery_timeout_ms),
        request_timeout_ms=(arguments.kafka_request_timeout_ms),
        poll_interval_ms=arguments.kafka_poll_interval_ms,
        compression_type=arguments.kafka_compression,
    )


def _emit(
    stream: TextIO,
    *,
    event: str,
    status: str,
    **fields: object,
) -> None:
    payload: dict[str, object] = {
        "timestamp": datetime.now(UTC).isoformat(timespec="milliseconds"),
        "event": event,
        "status": status,
    }
    payload.update(fields)
    json.dump(
        payload,
        stream,
        sort_keys=True,
        separators=(",", ":"),
    )
    stream.write("\n")
    stream.flush()


def _emit_report(
    stream: TextIO,
    report: OutboxPublicationReport,
) -> None:
    _emit(
        stream,
        event="outbox_publication_cycle_completed",
        status="ok",
        claimed=report.claimed,
        published=report.published,
        rescheduled=report.rescheduled,
        dead_lettered=report.dead_lettered,
    )


def _run_continuously(
    *,
    service: OutboxPublisherService,
    output: TextIO,
    idle_wait_ms: int,
) -> int:
    stop_requested = Event()

    def request_stop(
        _signal_number: int,
        _frame: FrameType | None,
    ) -> None:
        stop_requested.set()

    previous_sigterm = signal.signal(
        signal.SIGTERM,
        request_stop,
    )
    previous_sigint = signal.signal(
        signal.SIGINT,
        request_stop,
    )

    try:
        while not stop_requested.is_set():
            report = service.run_once()
            _emit_report(output, report)

            if not report.changed:
                stop_requested.wait(idle_wait_ms / 1_000)

        _emit(
            output,
            event="outbox_publisher_stopped",
            status="ok",
        )
        return EXIT_SUCCESS
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)
        signal.signal(signal.SIGINT, previous_sigint)


def main(
    argv: Sequence[str] | None = None,
    *,
    environment: Mapping[str, str] | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    """Run the outbox publisher with stable process exit codes."""

    effective_environment = os.environ if environment is None else environment
    output = sys.stdout if stdout is None else stdout
    error_output = sys.stderr if stderr is None else stderr
    arguments = _build_parser(effective_environment).parse_args(argv)
    pool: ConnectionPool[Connection[Any]] | None = None

    try:
        database_url = _database_url(effective_environment)
        _validate_process_limits(
            connect_timeout_seconds=(arguments.connect_timeout_seconds),
            idle_wait_ms=arguments.idle_wait_ms,
        )
        policy = _policy(arguments)
        kafka_configuration = _kafka_configuration(
            environment=effective_environment,
            arguments=arguments,
        )
        lease_ids = UuidLeaseIdGenerator(worker_id=arguments.worker_id)

        pool = ConnectionPool(
            conninfo=database_url,
            min_size=1,
            max_size=2,
            open=False,
            kwargs={
                "autocommit": True,
                "application_name": arguments.worker_id,
            },
        )
        pool.open(
            wait=True,
            timeout=float(arguments.connect_timeout_seconds),
        )

        service = OutboxPublisherService(
            store=PostgresOutboxStore(pool),
            publisher=ConfluentKafkaEventPublisher(kafka_configuration),
            clock=SystemUtcClock(),
            lease_ids=lease_ids,
            policy=policy,
        )

        if arguments.run_once:
            report = service.run_once()
            _emit_report(output, report)
            return EXIT_SUCCESS

        return _run_continuously(
            service=service,
            output=output,
            idle_wait_ms=arguments.idle_wait_ms,
        )

    except (
        PublisherCommandConfigurationError,
        KafkaPublisherConfigurationError,
        OutboxPublicationConfigurationError,
        ValueError,
    ):
        _emit(
            error_output,
            event="outbox_publisher_rejected",
            status="error",
            error_code="configuration_error",
        )
        return EXIT_CONFIGURATION

    except (
        OutboxPublicationPersistenceError,
        PostgresOutboxStoreError,
        PoolTimeout,
        psycopg.Error,
    ):
        _emit(
            error_output,
            event="outbox_publisher_failed",
            status="error",
            error_code="database_error",
        )
        return EXIT_DATABASE

    except OutboxPublicationConsistencyError:
        _emit(
            error_output,
            event="outbox_publisher_failed",
            status="error",
            error_code="consistency_error",
        )
        return EXIT_CONSISTENCY

    finally:
        if pool is not None:
            pool.close(timeout=5.0)


if __name__ == "__main__":
    raise SystemExit(main())
