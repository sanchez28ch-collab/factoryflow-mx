"""Unit tests for PostgreSQL migration-runner configuration."""

from pathlib import Path
from typing import Any, cast
from unittest import TestCase
from unittest.mock import Mock

from psycopg import Connection
from psycopg_pool import ConnectionPool

from factoryflow_batch.adapters.postgres_migrations import (
    MigrationExecutionReport,
    PostgresMigrationRunner,
)


class TestPostgresMigrationConfiguration(TestCase):
    """Validate configuration before any database access."""

    def setUp(self) -> None:
        self.pool = cast(
            ConnectionPool[Connection[Any]],
            Mock(),
        )

    def test_execution_report_exposes_change_state(self) -> None:
        self.assertTrue(
            MigrationExecutionReport(
                applied_versions=(1,),
                skipped_versions=(),
            ).changed
        )
        self.assertFalse(
            MigrationExecutionReport(
                applied_versions=(),
                skipped_versions=(1,),
            ).changed
        )

    def test_accepts_valid_configuration(self) -> None:
        runner = PostgresMigrationRunner(
            pool=self.pool,
            migration_directory=Path("sql/migrations"),
            component="p01-batch-integration",
        )

        self.assertIsInstance(runner, PostgresMigrationRunner)

    def test_rejects_invalid_component_names(self) -> None:
        invalid_components = (
            "",
            "ab",
            "P01-batch",
            "p01 batch",
            "p01.batch",
            "a" * 65,
        )

        for component in invalid_components:
            with (
                self.subTest(component=component),
                self.assertRaisesRegex(
                    ValueError,
                    "component has an invalid format",
                ),
            ):
                PostgresMigrationRunner(
                    pool=self.pool,
                    migration_directory=Path("sql/migrations"),
                    component=component,
                )

    def test_rejects_invalid_advisory_lock_keys(self) -> None:
        cases = (
            {"lock_namespace": True},
            {"lock_namespace": -(2**31) - 1},
            {"lock_namespace": 2**31},
            {"lock_key": False},
            {"lock_key": -(2**31) - 1},
            {"lock_key": 2**31},
        )

        for values in cases:
            with (
                self.subTest(values=values),
                self.assertRaisesRegex(
                    ValueError,
                    "signed 32-bit integer",
                ),
            ):
                PostgresMigrationRunner(
                    pool=self.pool,
                    migration_directory=Path("sql/migrations"),
                    component="p01-batch-integration",
                    **values,
                )

    def test_rejects_invalid_timeout_values(self) -> None:
        cases = (
            {"lock_timeout_ms": True},
            {"lock_timeout_ms": 0},
            {"lock_timeout_ms": 3_600_001},
            {"statement_timeout_ms": False},
            {"statement_timeout_ms": 0},
            {"statement_timeout_ms": 3_600_001},
        )

        for values in cases:
            with (
                self.subTest(values=values),
                self.assertRaisesRegex(
                    ValueError,
                    "between 1 and 3600000 milliseconds",
                ),
            ):
                PostgresMigrationRunner(
                    pool=self.pool,
                    migration_directory=Path("sql/migrations"),
                    component="p01-batch-integration",
                    **values,
                )
