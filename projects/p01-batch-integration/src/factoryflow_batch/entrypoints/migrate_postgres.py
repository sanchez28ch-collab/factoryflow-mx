"""Operational command for applying P01 PostgreSQL migrations."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final, TextIO

import psycopg
from psycopg import Connection
from psycopg.conninfo import conninfo_to_dict
from psycopg_pool import ConnectionPool, PoolTimeout

from factoryflow_batch.adapters.exceptions import (
    PostgresMigrationDiscoveryError,
    PostgresMigrationDriftError,
    PostgresMigrationError,
)
from factoryflow_batch.adapters.postgres_migrations import (
    PostgresMigrationRunner,
)

EXIT_SUCCESS: Final = 0
EXIT_CONFIGURATION: Final = 2
EXIT_DRIFT: Final = 3
EXIT_DATABASE: Final = 4

_DATABASE_URL_ENV: Final = "P01_DATABASE_URL"
_COMPONENT_ENV: Final = "P01_MIGRATION_COMPONENT"
_DIRECTORY_ENV: Final = "P01_MIGRATION_DIRECTORY"
_CONNECT_TIMEOUT_ENV: Final = "P01_DATABASE_CONNECT_TIMEOUT_SECONDS"
_LOCK_TIMEOUT_ENV: Final = "P01_MIGRATION_LOCK_TIMEOUT_MS"
_STATEMENT_TIMEOUT_ENV: Final = "P01_MIGRATION_STATEMENT_TIMEOUT_MS"

_DEFAULT_COMPONENT: Final = "p01-batch-integration"
_DEFAULT_CONNECT_TIMEOUT_SECONDS: Final = "10"
_DEFAULT_LOCK_TIMEOUT_MS: Final = "10000"
_DEFAULT_STATEMENT_TIMEOUT_MS: Final = "300000"
_DEFAULT_MIGRATION_DIRECTORY: Final = Path(__file__).resolve().parents[3] / "sql" / "migrations"


class MigrationCommandConfigurationError(ValueError):
    """Raised when the migration command configuration is invalid."""


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
        prog="factoryflow-p01-migrate",
        description=(
            "Apply verified FactoryFlow P01 PostgreSQL migrations. "
            "The database URL is read only from P01_DATABASE_URL."
        ),
    )
    parser.add_argument(
        "--migration-directory",
        type=Path,
        default=environment.get(
            _DIRECTORY_ENV,
            str(_DEFAULT_MIGRATION_DIRECTORY),
        ),
        help=(
            "Directory containing versioned SQL migrations. "
            "Defaults to P01_MIGRATION_DIRECTORY or the repository path."
        ),
    )
    parser.add_argument(
        "--component",
        default=environment.get(
            _COMPONENT_ENV,
            _DEFAULT_COMPONENT,
        ),
        help=("Migration-ledger component identifier. Defaults to P01_MIGRATION_COMPONENT."),
    )
    parser.add_argument(
        "--connect-timeout-seconds",
        type=_positive_integer,
        default=environment.get(
            _CONNECT_TIMEOUT_ENV,
            _DEFAULT_CONNECT_TIMEOUT_SECONDS,
        ),
        help="Maximum time to establish the migration database connection.",
    )
    parser.add_argument(
        "--lock-timeout-ms",
        type=_positive_integer,
        default=environment.get(
            _LOCK_TIMEOUT_ENV,
            _DEFAULT_LOCK_TIMEOUT_MS,
        ),
        help="PostgreSQL lock timeout used while applying migrations.",
    )
    parser.add_argument(
        "--statement-timeout-ms",
        type=_positive_integer,
        default=environment.get(
            _STATEMENT_TIMEOUT_ENV,
            _DEFAULT_STATEMENT_TIMEOUT_MS,
        ),
        help="PostgreSQL statement timeout used during migrations.",
    )
    return parser


def _database_url(environment: Mapping[str, str]) -> str:
    database_url = environment.get(_DATABASE_URL_ENV)

    if database_url is None or not database_url.strip():
        raise MigrationCommandConfigurationError(f"{_DATABASE_URL_ENV} is required.")

    try:
        conninfo_to_dict(database_url)
    except psycopg.ProgrammingError as error:
        raise MigrationCommandConfigurationError(f"{_DATABASE_URL_ENV} is invalid.") from error

    return database_url


def _validate_command_limits(
    *,
    connect_timeout_seconds: int,
    lock_timeout_ms: int,
    statement_timeout_ms: int,
) -> None:
    if connect_timeout_seconds > 300:
        raise MigrationCommandConfigurationError("connect timeout cannot exceed 300 seconds.")

    for name, value in (
        ("lock timeout", lock_timeout_ms),
        ("statement timeout", statement_timeout_ms),
    ):
        if value > 3_600_000:
            raise MigrationCommandConfigurationError(f"{name} cannot exceed 3600000 milliseconds.")


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


def main(
    argv: Sequence[str] | None = None,
    *,
    environment: Mapping[str, str] | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    """Execute migrations and return a stable process exit code."""

    effective_environment = os.environ if environment is None else environment
    output = sys.stdout if stdout is None else stdout
    error_output = sys.stderr if stderr is None else stderr
    parser = _build_parser(effective_environment)
    arguments = parser.parse_args(argv)

    pool: ConnectionPool[Connection[Any]] | None = None

    try:
        database_url = _database_url(effective_environment)
        _validate_command_limits(
            connect_timeout_seconds=arguments.connect_timeout_seconds,
            lock_timeout_ms=arguments.lock_timeout_ms,
            statement_timeout_ms=arguments.statement_timeout_ms,
        )

        pool = ConnectionPool(
            conninfo=database_url,
            min_size=1,
            max_size=1,
            open=False,
            kwargs={
                "autocommit": True,
                "application_name": "factoryflow-p01-migrations",
            },
        )
        pool.open(
            wait=True,
            timeout=float(arguments.connect_timeout_seconds),
        )

        runner = PostgresMigrationRunner(
            pool=pool,
            migration_directory=arguments.migration_directory,
            component=arguments.component,
            lock_timeout_ms=arguments.lock_timeout_ms,
            statement_timeout_ms=arguments.statement_timeout_ms,
        )
        report = runner.run()

        _emit(
            output,
            event="postgres_migration_completed",
            status="ok",
            component=arguments.component,
            changed=report.changed,
            applied_versions=list(report.applied_versions),
            skipped_versions=list(report.skipped_versions),
        )
        return EXIT_SUCCESS

    except (
        MigrationCommandConfigurationError,
        PostgresMigrationDiscoveryError,
        ValueError,
    ):
        _emit(
            error_output,
            event="postgres_migration_rejected",
            status="error",
            error_code="configuration_error",
        )
        return EXIT_CONFIGURATION

    except PostgresMigrationDriftError:
        _emit(
            error_output,
            event="postgres_migration_rejected",
            status="error",
            error_code="migration_drift",
        )
        return EXIT_DRIFT

    except (PostgresMigrationError, PoolTimeout, psycopg.Error):
        _emit(
            error_output,
            event="postgres_migration_failed",
            status="error",
            error_code="database_error",
        )
        return EXIT_DATABASE

    finally:
        if pool is not None:
            pool.close(timeout=5.0)


if __name__ == "__main__":
    raise SystemExit(main())
