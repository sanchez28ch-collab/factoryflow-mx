"""Unit tests for the PostgreSQL migration command."""

import json
from io import StringIO
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, patch

from psycopg_pool import PoolTimeout

import factoryflow_batch.entrypoints.migrate_postgres as command
from factoryflow_batch.adapters.exceptions import (
    PostgresMigrationDriftError,
    PostgresMigrationExecutionError,
)
from factoryflow_batch.adapters.postgres_migrations import (
    MigrationExecutionReport,
)

_DATABASE_URL = "postgresql://migration_user:private-value@localhost/test"


class TestPostgresMigrationCommand(TestCase):
    """Validate stable output, exit codes and credential redaction."""

    def setUp(self) -> None:
        self.stdout = StringIO()
        self.stderr = StringIO()
        self.environment = {
            "P01_DATABASE_URL": _DATABASE_URL,
        }

    def _execute(
        self,
        *,
        environment: dict[str, str] | None = None,
    ) -> int:
        return command.main(
            ["--migration-directory", "sql/migrations"],
            environment=(self.environment if environment is None else environment),
            stdout=self.stdout,
            stderr=self.stderr,
        )

    def test_rejects_missing_database_url(self) -> None:
        exit_code = self._execute(environment={})

        self.assertEqual(exit_code, command.EXIT_CONFIGURATION)
        self.assertEqual(self.stdout.getvalue(), "")
        payload = json.loads(self.stderr.getvalue())
        self.assertEqual(payload["error_code"], "configuration_error")

    def test_rejects_invalid_database_url_without_leaking_it(self) -> None:
        secret_value = "not-valid private-password"
        exit_code = self._execute(environment={"P01_DATABASE_URL": secret_value})

        self.assertEqual(exit_code, command.EXIT_CONFIGURATION)
        self.assertNotIn(secret_value, self.stderr.getvalue())
        self.assertNotIn("private-password", self.stderr.getvalue())

    @patch.object(command, "PostgresMigrationRunner")
    @patch.object(command, "ConnectionPool")
    def test_reports_successful_migration(
        self,
        pool_class: MagicMock,
        runner_class: MagicMock,
    ) -> None:
        pool = pool_class.return_value
        runner_class.return_value.run.return_value = MigrationExecutionReport(
            applied_versions=(1, 2),
            skipped_versions=(),
        )

        exit_code = self._execute()

        self.assertEqual(exit_code, command.EXIT_SUCCESS)
        self.assertEqual(self.stderr.getvalue(), "")
        pool.open.assert_called_once_with(wait=True, timeout=10.0)
        pool.close.assert_called_once_with(timeout=5.0)

        payload = json.loads(self.stdout.getvalue())
        self.assertEqual(payload["status"], "ok")
        self.assertTrue(payload["changed"])
        self.assertEqual(payload["applied_versions"], [1, 2])
        self.assertEqual(payload["skipped_versions"], [])
        self.assertNotIn("private-value", self.stdout.getvalue())

        runner_class.assert_called_once()
        runner_arguments = runner_class.call_args.kwargs
        self.assertEqual(
            runner_arguments["migration_directory"],
            Path("sql/migrations"),
        )
        self.assertEqual(
            runner_arguments["component"],
            "p01-batch-integration",
        )

    @patch.object(command, "PostgresMigrationRunner")
    @patch.object(command, "ConnectionPool")
    def test_returns_distinct_exit_code_for_drift(
        self,
        pool_class: MagicMock,
        runner_class: MagicMock,
    ) -> None:
        runner_class.return_value.run.side_effect = PostgresMigrationDriftError(
            "sensitive drift detail"
        )

        exit_code = self._execute()

        self.assertEqual(exit_code, command.EXIT_DRIFT)
        self.assertEqual(self.stdout.getvalue(), "")
        self.assertNotIn("sensitive drift detail", self.stderr.getvalue())
        payload = json.loads(self.stderr.getvalue())
        self.assertEqual(payload["error_code"], "migration_drift")
        pool_class.return_value.close.assert_called_once_with(timeout=5.0)

    @patch.object(command, "PostgresMigrationRunner")
    @patch.object(command, "ConnectionPool")
    def test_redacts_database_execution_failure(
        self,
        pool_class: MagicMock,
        runner_class: MagicMock,
    ) -> None:
        runner_class.return_value.run.side_effect = PostgresMigrationExecutionError(
            "database private diagnostic"
        )

        exit_code = self._execute()

        self.assertEqual(exit_code, command.EXIT_DATABASE)
        self.assertNotIn(
            "database private diagnostic",
            self.stderr.getvalue(),
        )
        payload = json.loads(self.stderr.getvalue())
        self.assertEqual(payload["error_code"], "database_error")
        pool_class.return_value.close.assert_called_once_with(timeout=5.0)

    @patch.object(command, "ConnectionPool")
    def test_reports_connection_timeout(
        self,
        pool_class: MagicMock,
    ) -> None:
        pool_class.return_value.open.side_effect = PoolTimeout("private connection detail")

        exit_code = self._execute()

        self.assertEqual(exit_code, command.EXIT_DATABASE)
        self.assertNotIn(
            "private connection detail",
            self.stderr.getvalue(),
        )
        payload = json.loads(self.stderr.getvalue())
        self.assertEqual(payload["error_code"], "database_error")
        pool_class.return_value.close.assert_called_once_with(timeout=5.0)

    def test_rejects_excessive_timeout_without_connecting(self) -> None:
        with patch.object(command, "ConnectionPool") as pool_class:
            exit_code = command.main(
                [
                    "--migration-directory",
                    "sql/migrations",
                    "--connect-timeout-seconds",
                    "301",
                ],
                environment=self.environment,
                stdout=self.stdout,
                stderr=self.stderr,
            )

        self.assertEqual(exit_code, command.EXIT_CONFIGURATION)
        pool_class.assert_not_called()
