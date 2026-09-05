"""Integration tests against a real PostgreSQL batch registry."""

import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from pathlib import Path
from unittest import TestCase, skipUnless
from unittest.mock import patch
from uuid import UUID, uuid4

import psycopg
from jsonschema import Draft202012Validator, FormatChecker
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from referencing import Registry, Resource

import factoryflow_batch.adapters.postgres_batch_registry as registry_module
from factoryflow_batch.adapters.exceptions import (
    PostgresBatchRegistryError,
)
from factoryflow_batch.adapters.postgres_batch_registry import (
    PostgresBatchRegistry,
)
from factoryflow_batch.domain import BatchManifest

_DATABASE_URL = os.environ.get("P01_TEST_DATABASE_URL")


def _manifest(
    batch_id: UUID | None = None,
) -> BatchManifest:
    return BatchManifest.create(
        batch_id=batch_id or uuid4(),
        source_system="erp",
        dataset="orders",
        schema_version="1.0.0",
        logical_date=date(2026, 9, 5),
        source_file_name="orders.csv",
        landing_uri="file:///landing/orders.csv",
        content_type="text/csv",
        extracted_at=datetime(2026, 9, 5, 10, 0, tzinfo=UTC),
        landed_at=datetime(2026, 9, 5, 10, 1, tzinfo=UTC),
        record_count=2,
        file_size_bytes=42,
        sha256="a" * 64,
    )


@skipUnless(
    _DATABASE_URL is not None,
    "P01_TEST_DATABASE_URL is required for PostgreSQL integration tests.",
)
class TestPostgresBatchRegistryIntegration(TestCase):
    pool: ConnectionPool
    event_validator: Draft202012Validator

    @classmethod
    def setUpClass(cls) -> None:
        assert _DATABASE_URL is not None

        project_root = Path(__file__).resolve().parents[2]
        repository_root = Path(__file__).resolve().parents[4]

        event_schema = json.loads(
            (
                repository_root
                / "shared"
                / "contracts"
                / "jsonschema"
                / "batch"
                / "batch-ingested-v1.schema.json"
            ).read_text(encoding="utf-8")
        )
        manifest_schema = json.loads(
            (
                repository_root
                / "shared"
                / "contracts"
                / "jsonschema"
                / "batch"
                / "batch-manifest-v1.schema.json"
            ).read_text(encoding="utf-8")
        )
        registry = Registry().with_resources(
            (
                (
                    manifest_schema["$id"],
                    Resource.from_contents(manifest_schema),
                ),
                (
                    event_schema["$id"],
                    Resource.from_contents(event_schema),
                ),
            )
        )
        cls.event_validator = Draft202012Validator(
            event_schema,
            registry=registry,
            format_checker=FormatChecker(),
        )

        migration_file = (
            project_root / "sql" / "migrations" / "001_create_batch_registry_outbox.sql"
        )
        migration = migration_file.read_text(encoding="utf-8")

        with psycopg.connect(_DATABASE_URL) as connection:
            connection.execute(
                migration,
                prepare=False,
            )

        cls.pool = ConnectionPool(
            conninfo=_DATABASE_URL,
            min_size=1,
            max_size=8,
            timeout=10,
            kwargs={
                "autocommit": True,
                "application_name": "factoryflow-p01-integration-tests",
            },
            open=False,
        )
        cls.pool.open(wait=True, timeout=15)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.pool.close()

    def setUp(self) -> None:
        self._truncate_tables()

    def tearDown(self) -> None:
        self._truncate_tables()

    def _truncate_tables(self) -> None:
        with self.pool.connection() as connection:
            connection.execute(
                """
                TRUNCATE TABLE
                    ingestion.outbox_events,
                    ingestion.batch_manifests
                """
            )

    def _counts(self) -> tuple[int, int]:
        with (
            self.pool.connection() as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute(
                """
                SELECT
                    (SELECT count(*) FROM ingestion.batch_manifests),
                    (SELECT count(*) FROM ingestion.outbox_events)
                """
            )
            row = cursor.fetchone()

        assert row is not None
        return int(row[0]), int(row[1])

    def _outbox_row(self) -> dict[str, object]:
        with (
            self.pool.connection() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                SELECT
                    event_id,
                    aggregate_type,
                    aggregate_id,
                    event_type,
                    event_version,
                    destination_topic,
                    partition_key,
                    payload,
                    headers,
                    occurred_at,
                    attempt_count,
                    published_at,
                    dead_lettered_at
                FROM ingestion.outbox_events
                """
            )
            row = cursor.fetchone()

        assert row is not None
        return row

    def test_registers_manifest_and_outbox_event_atomically(self) -> None:
        manifest = _manifest()
        registry = PostgresBatchRegistry(self.pool)

        registration = registry.register_if_absent(manifest)
        loaded = registry.find_by_idempotency_key(manifest.idempotency_key)
        outbox = self._outbox_row()

        self.assertTrue(registration.created)
        self.assertEqual(registration.manifest, manifest)
        self.assertEqual(loaded, manifest)
        self.assertEqual(self._counts(), (1, 1))

        self.assertEqual(outbox["aggregate_type"], "batch")
        self.assertEqual(outbox["aggregate_id"], manifest.batch_id)
        self.assertEqual(
            outbox["event_type"],
            "factoryflow.batch.ingested",
        )
        self.assertEqual(outbox["event_version"], 1)
        self.assertEqual(
            outbox["destination_topic"],
            "factoryflow.batch.ingested.v1",
        )
        self.assertEqual(
            outbox["partition_key"],
            str(manifest.batch_id),
        )
        self.assertEqual(outbox["attempt_count"], 0)
        self.assertIsNone(outbox["published_at"])
        self.assertIsNone(outbox["dead_lettered_at"])

        payload = outbox["payload"]
        headers = outbox["headers"]

        self.assertIsInstance(payload, dict)
        self.assertIsInstance(headers, dict)

        assert isinstance(payload, dict)
        assert isinstance(headers, dict)

        contract_errors = sorted(
            self.event_validator.iter_errors(payload),
            key=lambda error: list(error.absolute_path),
        )
        self.assertEqual(contract_errors, [])

        self.assertEqual(
            payload["event_id"],
            str(outbox["event_id"]),
        )
        self.assertEqual(
            payload["aggregate_id"],
            str(manifest.batch_id),
        )
        self.assertEqual(
            payload["event_type"],
            "factoryflow.batch.ingested",
        )
        self.assertEqual(
            payload["data"]["idempotency_key"],
            manifest.idempotency_key,
        )
        self.assertEqual(
            headers["event_schema"],
            "urn:factoryflow:events:batch-ingested:1.0.0",
        )

    def test_retry_returns_winner_and_does_not_duplicate_outbox(self) -> None:
        registry = PostgresBatchRegistry(self.pool)
        first = _manifest(UUID("a32a8d26-4beef4c2-8f21-000000000011"))
        retry = _manifest(UUID("a32a8d26-4beef4c2-8f21-000000000012"))

        first_result = registry.register_if_absent(first)
        retry_result = registry.register_if_absent(retry)

        self.assertTrue(first_result.created)
        self.assertFalse(retry_result.created)
        self.assertEqual(retry_result.manifest, first)
        self.assertEqual(self._counts(), (1, 1))

    def test_concurrent_registrations_produce_one_winner(self) -> None:
        registry = PostgresBatchRegistry(self.pool)
        candidates = [_manifest() for _ in range(8)]

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(
                executor.map(
                    registry.register_if_absent,
                    candidates,
                )
            )

        created = [result for result in results if result.created]
        winning_ids = {result.manifest.batch_id for result in results}

        self.assertEqual(len(created), 1)
        self.assertEqual(len(winning_ids), 1)
        self.assertEqual(self._counts(), (1, 1))

    def test_outbox_failure_rolls_back_manifest(self) -> None:
        registry = PostgresBatchRegistry(self.pool)
        manifest = _manifest()

        with (
            patch.object(
                registry_module,
                "_DESTINATION_TOPIC",
                "invalid topic",
            ),
            self.assertRaisesRegex(
                PostgresBatchRegistryError,
                "PostgreSQL batch registration failed",
            ),
        ):
            registry.register_if_absent(manifest)

        self.assertIsNone(registry.find_by_idempotency_key(manifest.idempotency_key))
        self.assertEqual(self._counts(), (0, 0))

    def test_batch_manifest_cannot_be_updated_or_deleted(self) -> None:
        registry = PostgresBatchRegistry(self.pool)
        manifest = _manifest()
        registry.register_if_absent(manifest)

        statements = (
            """
            UPDATE ingestion.batch_manifests
            SET record_count = record_count + 1
            WHERE batch_id = %s
            """,
            """
            DELETE FROM ingestion.batch_manifests
            WHERE batch_id = %s
            """,
        )

        for statement in statements:
            with (
                self.subTest(statement=statement.strip().split()[0]),
                self.assertRaises(psycopg.errors.ObjectNotInPrerequisiteState),
                self.pool.connection() as connection,
                connection.transaction(),
            ):
                connection.execute(
                    statement,
                    (manifest.batch_id,),
                )

        self.assertEqual(self._counts(), (1, 1))
