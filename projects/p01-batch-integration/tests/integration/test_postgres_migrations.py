"""Integration tests for the PostgreSQL migration runner."""

import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, skipUnless
from uuid import uuid4

from psycopg import sql
from psycopg_pool import ConnectionPool

from factoryflow_batch.adapters.exceptions import (
    PostgresMigrationDriftError,
    PostgresMigrationExecutionError,
)
from factoryflow_batch.adapters.postgres_migrations import (
    MigrationExecutionReport,
    PostgresMigrationRunner,
)

_DATABASE_URL = os.environ.get("P01_TEST_DATABASE_URL")
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_REPOSITORY_MIGRATIONS = _PROJECT_ROOT / "sql" / "migrations"


@skipUnless(
    _DATABASE_URL,
    "P01_TEST_DATABASE_URL is required for PostgreSQL integration tests.",
)
class TestPostgresMigrationRunnerIntegration(TestCase):
    """Exercise migration guarantees against an isolated real database."""

    pool: ConnectionPool

    @classmethod
    def setUpClass(cls) -> None:
        if _DATABASE_URL is None:
            raise RuntimeError("P01_TEST_DATABASE_URL is required.")

        cls.pool = ConnectionPool(
            conninfo=_DATABASE_URL,
            min_size=1,
            max_size=6,
            kwargs={"autocommit": True},
            open=True,
        )
        cls.pool.wait()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.pool.close()

    def setUp(self) -> None:
        unique_suffix = uuid4().hex
        self.component = f"p01-migration-test-{unique_suffix}"
        self.schema = f"p01_migration_{unique_suffix}"

    def tearDown(self) -> None:
        with self.pool.connection() as connection:
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(self.schema))
            )

            ledger_exists = connection.execute(
                "SELECT to_regclass('audit.schema_migrations')"
            ).fetchone()

            if ledger_exists is not None and ledger_exists[0] is not None:
                connection.execute(
                    """
                    DELETE FROM audit.schema_migrations
                    WHERE component = %s
                    """,
                    (self.component,),
                )

    def _runner(self, migration_directory: Path) -> PostgresMigrationRunner:
        return PostgresMigrationRunner(
            pool=self.pool,
            migration_directory=migration_directory,
            component=self.component,
            lock_namespace=7001,
            lock_key=20,
        )

    def _write_migration(
        self,
        directory: Path,
        *,
        version: int,
        name: str,
        statement: str,
    ) -> Path:
        migration = directory / f"{version:03d}_{name}.sql"
        migration.write_text(
            statement.rstrip() + "\n",
            encoding="utf-8",
        )
        return migration

    def _create_probe_migration(self, directory: Path) -> Path:
        return self._write_migration(
            directory,
            version=1,
            name="create_probe_table",
            statement=f"""
            CREATE SCHEMA {self.schema};

            CREATE TABLE {self.schema}.probe (
                id INTEGER PRIMARY KEY,
                value TEXT NOT NULL
            );
            """,
        )

    def _ledger_count(self) -> int:
        with self.pool.connection() as connection:
            ledger = connection.execute("SELECT to_regclass('audit.schema_migrations')").fetchone()

            if ledger is None or ledger[0] is None:
                return 0

            row = connection.execute(
                """
                SELECT COUNT(*)
                FROM audit.schema_migrations
                WHERE component = %s
                """,
                (self.component,),
            ).fetchone()

        if row is None:
            raise AssertionError("The migration ledger count was not returned.")

        return int(row[0])

    def test_applies_migration_and_skips_verified_retry(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            migration = self._create_probe_migration(directory)
            runner = self._runner(directory)

            first_report = runner.run()
            retry_report = runner.run()

            self.assertEqual(
                first_report,
                MigrationExecutionReport(
                    applied_versions=(1,),
                    skipped_versions=(),
                ),
            )
            self.assertTrue(first_report.changed)
            self.assertEqual(
                retry_report,
                MigrationExecutionReport(
                    applied_versions=(),
                    skipped_versions=(1,),
                ),
            )
            self.assertFalse(retry_report.changed)

            with self.pool.connection() as connection:
                table = connection.execute(
                    "SELECT to_regclass(%s)",
                    (f"{self.schema}.probe",),
                ).fetchone()
                ledger = connection.execute(
                    """
                    SELECT version, name, checksum, execution_ms,
                           installed_by, database_name
                    FROM audit.schema_migrations
                    WHERE component = %s
                    """,
                    (self.component,),
                ).fetchone()

            self.assertIsNotNone(table)
            self.assertEqual(table[0], f"{self.schema}.probe")
            self.assertIsNotNone(ledger)
            self.assertEqual(ledger[0], 1)
            self.assertEqual(ledger[1], "create_probe_table")
            self.assertEqual(
                ledger[2],
                hashlib.sha256(migration.read_bytes()).hexdigest(),
            )
            self.assertRegex(ledger[2], r"^[a-f0-9]{64}$")
            self.assertGreaterEqual(ledger[3], 0)
            self.assertTrue(ledger[4])
            self.assertTrue(ledger[5])

    def test_detects_checksum_drift_without_modifying_database(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            migration = self._create_probe_migration(directory)
            runner = self._runner(directory)
            runner.run()

            migration.write_text(
                migration.read_text(encoding="utf-8") + "-- unauthorized historical modification\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                PostgresMigrationDriftError,
                "differs from the repository",
            ):
                runner.run()

            self.assertEqual(self._ledger_count(), 1)

    def test_detects_installed_migration_missing_from_repository(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            migration = self._create_probe_migration(directory)
            runner = self._runner(directory)
            runner.run()
            migration.unlink()

            with self.assertRaisesRegex(
                PostgresMigrationDriftError,
                "missing from the repository",
            ):
                runner.run()

            self.assertEqual(self._ledger_count(), 1)

    def test_failed_migration_rolls_back_entire_pending_batch(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            self._create_probe_migration(directory)
            self._write_migration(
                directory,
                version=2,
                name="write_missing_table",
                statement=f"""
                INSERT INTO {self.schema}.table_that_does_not_exist (id)
                VALUES (1);
                """,
            )

            with self.assertRaisesRegex(
                PostgresMigrationExecutionError,
                "002_write_missing_table.sql",
            ):
                self._runner(directory).run()

            with self.pool.connection() as connection:
                schema = connection.execute(
                    "SELECT to_regnamespace(%s)",
                    (self.schema,),
                ).fetchone()

            self.assertIsNotNone(schema)
            self.assertIsNone(schema[0])
            self.assertEqual(self._ledger_count(), 0)

    def test_concurrent_runners_apply_each_version_once(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            self._create_probe_migration(directory)
            runner = self._runner(directory)

            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [executor.submit(runner.run) for _ in range(2)]
                reports = [future.result(timeout=30) for future in futures]

            self.assertCountEqual(
                [report.applied_versions for report in reports],
                [(1,), ()],
            )
            self.assertCountEqual(
                [report.skipped_versions for report in reports],
                [(), (1,)],
            )
            self.assertEqual(self._ledger_count(), 1)

    def test_repository_migration_is_runner_compatible(self) -> None:
        report = self._runner(_REPOSITORY_MIGRATIONS).run()

        self.assertEqual(report.applied_versions, (1,))
        self.assertEqual(report.skipped_versions, ())

        with self.pool.connection() as connection:
            objects = connection.execute(
                """
                SELECT
                    to_regclass('ingestion.batch_manifests'),
                    to_regclass('ingestion.outbox_events')
                """
            ).fetchone()

        self.assertIsNotNone(objects)
        self.assertEqual(objects[0], "ingestion.batch_manifests")
        self.assertEqual(objects[1], "ingestion.outbox_events")
        self.assertEqual(self._ledger_count(), 1)
