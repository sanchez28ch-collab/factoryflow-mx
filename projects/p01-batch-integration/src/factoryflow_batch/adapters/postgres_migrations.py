"""Deterministic PostgreSQL migration discovery and execution."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter_ns
from typing import Any, Final

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from factoryflow_batch.adapters.exceptions import (
    PostgresMigrationDiscoveryError,
    PostgresMigrationDriftError,
    PostgresMigrationError,
    PostgresMigrationExecutionError,
)

_MIGRATION_NAME: Final = re.compile(r"^(?P<version>[0-9]{3})_(?P<name>[a-z][a-z0-9_]{2,79})\.sql$")
_COMPONENT_NAME: Final = re.compile(r"^[a-z][a-z0-9_-]{2,63}$")
_CHECKSUM: Final = re.compile(r"^[a-f0-9]{64}$")
_MIN_LOCK_KEY: Final = -(2**31)
_MAX_LOCK_KEY: Final = 2**31 - 1

_CREATE_AUDIT_SCHEMA: Final = "CREATE SCHEMA IF NOT EXISTS audit"

_CREATE_MIGRATION_LEDGER: Final = """
CREATE TABLE IF NOT EXISTS audit.schema_migrations (
    component TEXT NOT NULL,
    version INTEGER NOT NULL,
    name TEXT NOT NULL,
    checksum CHARACTER(64) NOT NULL,
    execution_ms BIGINT NOT NULL,
    installed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    installed_by TEXT NOT NULL DEFAULT CURRENT_USER,
    database_name TEXT NOT NULL DEFAULT CURRENT_DATABASE(),
    PRIMARY KEY (component, version),
    CONSTRAINT schema_migrations_component_ck
        CHECK (component ~ '^[a-z][a-z0-9_-]{2,63}$'),
    CONSTRAINT schema_migrations_version_ck
        CHECK (version BETWEEN 1 AND 999),
    CONSTRAINT schema_migrations_name_ck
        CHECK (name ~ '^[a-z][a-z0-9_]{2,79}$'),
    CONSTRAINT schema_migrations_checksum_ck
        CHECK (checksum ~ '^[a-f0-9]{64}$'),
    CONSTRAINT schema_migrations_execution_ms_ck
        CHECK (execution_ms >= 0)
)
"""

_SELECT_INSTALLED: Final = """
SELECT version, name, checksum
FROM audit.schema_migrations
WHERE component = %s
ORDER BY version
"""

_INSERT_INSTALLED: Final = """
INSERT INTO audit.schema_migrations (
    component,
    version,
    name,
    checksum,
    execution_ms
)
VALUES (%s, %s, %s, %s, %s)
"""


@dataclass(frozen=True, slots=True)
class SqlMigration:
    """One immutable SQL migration discovered from the repository."""

    version: int
    name: str
    path: Path
    checksum: str
    statement: str


@dataclass(frozen=True, slots=True)
class MigrationExecutionReport:
    """Versions applied and skipped during one migration execution."""

    applied_versions: tuple[int, ...]
    skipped_versions: tuple[int, ...]

    @property
    def changed(self) -> bool:
        """Report whether PostgreSQL was modified."""

        return bool(self.applied_versions)


def discover_sql_migrations(
    migration_directory: Path,
) -> tuple[SqlMigration, ...]:
    """Discover ordered, regular, UTF-8 SQL migration artifacts."""

    if migration_directory.is_symlink():
        raise PostgresMigrationDiscoveryError("The migration directory cannot be a symbolic link.")

    if not migration_directory.is_dir():
        raise PostgresMigrationDiscoveryError(
            "The migration directory does not exist or is not a directory."
        )

    try:
        candidates = sorted(
            (path for path in migration_directory.iterdir() if path.suffix == ".sql"),
            key=lambda path: path.name,
        )
    except OSError as error:
        raise PostgresMigrationDiscoveryError(
            "The migration directory could not be read."
        ) from error

    migrations: list[SqlMigration] = []
    observed_versions: set[int] = set()

    for path in candidates:
        if path.is_symlink() or not path.is_file():
            raise PostgresMigrationDiscoveryError(
                f"Migration artifact {path.name!r} must be a regular file."
            )

        match = _MIGRATION_NAME.fullmatch(path.name)

        if match is None:
            raise PostgresMigrationDiscoveryError(f"Migration filename {path.name!r} is invalid.")

        version = int(match.group("version"))

        if version == 0:
            raise PostgresMigrationDiscoveryError(
                "Migration version 000 is reserved and cannot be used."
            )

        if version in observed_versions:
            raise PostgresMigrationDiscoveryError(f"Migration version {version:03d} is duplicated.")

        try:
            raw_statement = path.read_bytes()
            statement = raw_statement.decode("utf-8")
        except (OSError, UnicodeDecodeError) as error:
            raise PostgresMigrationDiscoveryError(
                f"Migration artifact {path.name!r} is not readable UTF-8."
            ) from error

        if not statement.strip():
            raise PostgresMigrationDiscoveryError(f"Migration artifact {path.name!r} is empty.")

        checksum = hashlib.sha256(raw_statement).hexdigest()

        if _CHECKSUM.fullmatch(checksum) is None:
            raise PostgresMigrationDiscoveryError(
                f"Migration artifact {path.name!r} has an invalid checksum."
            )

        migrations.append(
            SqlMigration(
                version=version,
                name=match.group("name"),
                path=path.resolve(strict=True),
                checksum=checksum,
                statement=statement,
            )
        )
        observed_versions.add(version)

    return tuple(sorted(migrations, key=lambda migration: migration.version))


class PostgresMigrationRunner:
    """Apply immutable SQL migrations under a PostgreSQL advisory lock."""

    def __init__(
        self,
        *,
        pool: ConnectionPool[Connection[Any]],
        migration_directory: Path,
        component: str,
        lock_namespace: int = 7001,
        lock_key: int = 10,
        lock_timeout_ms: int = 10_000,
        statement_timeout_ms: int = 300_000,
    ) -> None:
        if _COMPONENT_NAME.fullmatch(component) is None:
            raise ValueError("component has an invalid format.")

        for name, value in (
            ("lock_namespace", lock_namespace),
            ("lock_key", lock_key),
        ):
            if isinstance(value, bool) or value < _MIN_LOCK_KEY or value > _MAX_LOCK_KEY:
                raise ValueError(f"{name} must be a signed 32-bit integer.")

        for name, value in (
            ("lock_timeout_ms", lock_timeout_ms),
            ("statement_timeout_ms", statement_timeout_ms),
        ):
            if isinstance(value, bool) or not 1 <= value <= 3_600_000:
                raise ValueError(f"{name} must be between 1 and 3600000 milliseconds.")

        self._pool = pool
        self._migration_directory = migration_directory
        self._component = component
        self._lock_namespace = lock_namespace
        self._lock_key = lock_key
        self._lock_timeout_ms = lock_timeout_ms
        self._statement_timeout_ms = statement_timeout_ms

    def run(self) -> MigrationExecutionReport:
        """Validate drift and atomically apply every pending migration."""

        migrations = discover_sql_migrations(self._migration_directory)
        migrations_by_version = {migration.version: migration for migration in migrations}
        applied_versions: list[int] = []
        skipped_versions: list[int] = []

        try:
            with (
                self._pool.connection() as connection,
                connection.transaction(),
                connection.cursor(row_factory=dict_row) as cursor,
            ):
                cursor.execute(
                    "SELECT set_config('lock_timeout', %s, true)",
                    (f"{self._lock_timeout_ms}ms",),
                )
                cursor.execute(
                    "SELECT set_config('statement_timeout', %s, true)",
                    (f"{self._statement_timeout_ms}ms",),
                )
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(%s, %s)",
                    (self._lock_namespace, self._lock_key),
                )
                cursor.execute(_CREATE_AUDIT_SCHEMA)
                cursor.execute(_CREATE_MIGRATION_LEDGER)
                cursor.execute(_SELECT_INSTALLED, (self._component,))

                installed_rows = cursor.fetchall()
                installed_versions: set[int] = set()

                for row in installed_rows:
                    version = int(row["version"])
                    installed_versions.add(version)
                    migration = migrations_by_version.get(version)

                    if migration is None:
                        raise PostgresMigrationDriftError(
                            f"An installed migration is missing from the repository: {version:03d}."
                        )

                    if (
                        str(row["name"]) != migration.name
                        or str(row["checksum"]) != migration.checksum
                    ):
                        raise PostgresMigrationDriftError(
                            "An installed migration differs from the repository: "
                            f"{version:03d}_{migration.name}.sql."
                        )

                    skipped_versions.append(version)

                for migration in migrations:
                    if migration.version in installed_versions:
                        continue

                    started_at = perf_counter_ns()

                    try:
                        cursor.execute(
                            migration.statement,
                            prepare=False,
                        )
                    except psycopg.Error as error:
                        raise PostgresMigrationExecutionError(
                            f"PostgreSQL rejected migration {migration.path.name!r}."
                        ) from error

                    elapsed_ns = perf_counter_ns() - started_at
                    execution_ms = max(0, (elapsed_ns + 999_999) // 1_000_000)

                    cursor.execute(
                        _INSERT_INSTALLED,
                        (
                            self._component,
                            migration.version,
                            migration.name,
                            migration.checksum,
                            execution_ms,
                        ),
                    )
                    applied_versions.append(migration.version)

        except PostgresMigrationError:
            raise
        except psycopg.Error as error:
            raise PostgresMigrationError("The PostgreSQL migration operation failed.") from error

        return MigrationExecutionReport(
            applied_versions=tuple(applied_versions),
            skipped_versions=tuple(skipped_versions),
        )
