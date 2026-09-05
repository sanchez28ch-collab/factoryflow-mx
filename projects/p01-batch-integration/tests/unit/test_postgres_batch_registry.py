"""Unit tests for the PostgreSQL batch registry adapter."""

from datetime import UTC, date, datetime
from unittest import TestCase
from uuid import UUID

import psycopg

from factoryflow_batch.adapters.exceptions import (
    PostgresBatchRegistryDataError,
    PostgresBatchRegistryError,
)
from factoryflow_batch.adapters.postgres_batch_registry import (
    PostgresBatchRegistry,
)
from factoryflow_batch.domain import BatchManifest


class _FakeCursor:
    def __init__(
        self,
        responses: list[dict[str, object] | None],
        *,
        failure_on_call: int | None = None,
    ) -> None:
        self._responses = responses
        self._failure_on_call = failure_on_call
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(
        self,
        query: str,
        params: tuple[object, ...],
    ) -> None:
        self.calls.append((query, params))

        if self._failure_on_call == len(self.calls):
            raise psycopg.OperationalError("simulated database failure")

    def fetchone(self) -> dict[str, object] | None:
        if not self._responses:
            return None

        return self._responses.pop(0)


class _CursorContext:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> _FakeCursor:
        return self._cursor

    def __exit__(
        self,
        exception_type: object,
        exception: object,
        traceback: object,
    ) -> None:
        return None


class _TransactionContext:
    def __enter__(self) -> None:
        return None

    def __exit__(
        self,
        exception_type: object,
        exception: object,
        traceback: object,
    ) -> None:
        return None


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(
        self,
        *,
        row_factory: object,
    ) -> _CursorContext:
        return _CursorContext(self._cursor)

    def transaction(self) -> _TransactionContext:
        return _TransactionContext()


class _ConnectionContext:
    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection

    def __enter__(self) -> _FakeConnection:
        return self._connection

    def __exit__(
        self,
        exception_type: object,
        exception: object,
        traceback: object,
    ) -> None:
        return None


class _FakePool:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._connection = _FakeConnection(cursor)

    def connection(self) -> _ConnectionContext:
        return _ConnectionContext(self._connection)


def _manifest(
    batch_id: str = "a32a8d26-4beef4c2-8f21-000000000001",
) -> BatchManifest:
    return BatchManifest.create(
        batch_id=UUID(batch_id),
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


def _row(manifest: BatchManifest) -> dict[str, object]:
    return {
        "batch_id": manifest.batch_id,
        "idempotency_key": manifest.idempotency_key,
        "manifest_version": manifest.manifest_version,
        "source_system": manifest.source_system,
        "dataset": manifest.dataset,
        "schema_version": manifest.schema_version,
        "logical_date": manifest.logical_date,
        "source_file_name": manifest.source_file_name,
        "landing_uri": manifest.landing_uri,
        "content_type": manifest.content_type,
        "extracted_at": manifest.extracted_at,
        "landed_at": manifest.landed_at,
        "record_count": manifest.record_count,
        "file_size_bytes": manifest.file_size_bytes,
        "sha256": manifest.sha256,
    }


class TestPostgresBatchRegistry(TestCase):
    def test_find_returns_none_when_registration_does_not_exist(self) -> None:
        cursor = _FakeCursor([None])
        registry = PostgresBatchRegistry(_FakePool(cursor))  # type: ignore[arg-type]

        result = registry.find_by_idempotency_key("a" * 64)

        self.assertIsNone(result)
        self.assertEqual(len(cursor.calls), 1)
        self.assertIn("WHERE idempotency_key = %s", cursor.calls[0][0])
        self.assertEqual(cursor.calls[0][1], ("a" * 64,))

    def test_find_reconstructs_persisted_manifest(self) -> None:
        manifest = _manifest()
        cursor = _FakeCursor([_row(manifest)])
        registry = PostgresBatchRegistry(_FakePool(cursor))  # type: ignore[arg-type]

        result = registry.find_by_idempotency_key(manifest.idempotency_key)

        self.assertEqual(result, manifest)

    def test_register_creates_manifest_and_outbox_event(self) -> None:
        manifest = _manifest()
        cursor = _FakeCursor([_row(manifest)])
        registry = PostgresBatchRegistry(_FakePool(cursor))  # type: ignore[arg-type]

        registration = registry.register_if_absent(manifest)

        self.assertTrue(registration.created)
        self.assertEqual(registration.manifest, manifest)
        self.assertEqual(len(cursor.calls), 2)

        manifest_query, manifest_params = cursor.calls[0]
        outbox_query, outbox_params = cursor.calls[1]

        self.assertIn("INSERT INTO ingestion.batch_manifests", manifest_query)
        self.assertEqual(manifest_params[0], manifest.batch_id)
        self.assertEqual(manifest_params[1], manifest.idempotency_key)

        self.assertIn("INSERT INTO ingestion.outbox_events", outbox_query)
        self.assertIsInstance(outbox_params[0], UUID)
        self.assertEqual(outbox_params[1], "batch")
        self.assertEqual(outbox_params[2], manifest.batch_id)
        self.assertEqual(outbox_params[3], "factoryflow.batch.ingested")
        self.assertEqual(outbox_params[4], 1)
        self.assertEqual(
            outbox_params[5],
            "factoryflow.batch.ingested.v1",
        )
        self.assertEqual(outbox_params[6], str(manifest.batch_id))
        self.assertEqual(outbox_params[9], manifest.landed_at)

    def test_register_returns_concurrent_winner_without_second_event(self) -> None:
        winner = _manifest()
        candidate = _manifest(
            "a32a8d26-4beef4c2-8f21-000000000002",
        )
        cursor = _FakeCursor([None, _row(winner)])
        registry = PostgresBatchRegistry(_FakePool(cursor))  # type: ignore[arg-type]

        registration = registry.register_if_absent(candidate)

        self.assertFalse(registration.created)
        self.assertEqual(registration.manifest, winner)
        self.assertEqual(len(cursor.calls), 2)
        self.assertIn("INSERT INTO ingestion.batch_manifests", cursor.calls[0][0])
        self.assertIn("WHERE idempotency_key = %s", cursor.calls[1][0])

    def test_register_rejects_missing_concurrent_winner(self) -> None:
        cursor = _FakeCursor([None, None])
        registry = PostgresBatchRegistry(_FakePool(cursor))  # type: ignore[arg-type]

        with self.assertRaisesRegex(
            PostgresBatchRegistryDataError,
            "conflicting batch registration could not be read",
        ):
            registry.register_if_absent(_manifest())

    def test_rejects_invalid_lookup_keys_before_database_access(self) -> None:
        cursor = _FakeCursor([])
        registry = PostgresBatchRegistry(_FakePool(cursor))  # type: ignore[arg-type]

        invalid_keys = (
            "",
            "a" * 63,
            "A" * 64,
            "g" * 64,
            "a" * 65,
        )

        for invalid_key in invalid_keys:
            with (
                self.subTest(invalid_key=invalid_key),
                self.assertRaisesRegex(
                    ValueError,
                    "lowercase hexadecimal SHA-256",
                ),
            ):
                registry.find_by_idempotency_key(invalid_key)

        self.assertEqual(cursor.calls, [])

    def test_rejects_corrupted_persisted_manifest(self) -> None:
        manifest = _manifest()
        corrupted_row = _row(manifest)
        corrupted_row["record_count"] = -1

        cursor = _FakeCursor([corrupted_row])
        registry = PostgresBatchRegistry(_FakePool(cursor))  # type: ignore[arg-type]

        with self.assertRaisesRegex(
            PostgresBatchRegistryDataError,
            "violates the domain contract",
        ):
            registry.find_by_idempotency_key(manifest.idempotency_key)

    def test_translates_psycopg_failures_without_exposing_details(self) -> None:
        scenarios = (
            (
                "lookup",
                lambda registry: registry.find_by_idempotency_key("a" * 64),
                "PostgreSQL batch lookup failed",
            ),
            (
                "registration",
                lambda registry: registry.register_if_absent(_manifest()),
                "PostgreSQL batch registration failed",
            ),
        )

        for name, operation, message in scenarios:
            with self.subTest(operation=name):
                cursor = _FakeCursor([], failure_on_call=1)
                registry = PostgresBatchRegistry(  # type: ignore[arg-type]
                    _FakePool(cursor)
                )

                with self.assertRaisesRegex(
                    PostgresBatchRegistryError,
                    message,
                ) as raised:
                    operation(registry)

                self.assertIsInstance(
                    raised.exception.__cause__,
                    psycopg.OperationalError,
                )
                self.assertNotIn(
                    "simulated database failure",
                    str(raised.exception),
                )

    def test_outbox_failure_is_translated(self) -> None:
        manifest = _manifest()
        cursor = _FakeCursor(
            [_row(manifest)],
            failure_on_call=2,
        )
        registry = PostgresBatchRegistry(_FakePool(cursor))  # type: ignore[arg-type]

        with self.assertRaisesRegex(
            PostgresBatchRegistryError,
            "PostgreSQL batch registration failed",
        ):
            registry.register_if_absent(manifest)
