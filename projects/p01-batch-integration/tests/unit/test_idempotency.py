"""Unit tests for deterministic batch identities."""

from datetime import UTC, date, datetime
from unittest import TestCase

from factoryflow_batch.domain import (
    BatchManifestValidationError,
    build_batch_idempotency_key,
)


class TestBatchIdempotencyKey(TestCase):
    """Verify stability, separation and input validation."""

    def build_key(self, **overrides: object) -> str:
        parameters = {
            "source_system": "erp_factory",
            "dataset": "production_orders",
            "logical_date": date(2026, 9, 5),
            "schema_version": "1.0.0",
            "artifact_sha256": "a" * 64,
        }
        parameters.update(overrides)
        return build_batch_idempotency_key(**parameters)  # type: ignore[arg-type]

    def test_matches_golden_value(self) -> None:
        expected = "01912fd29cd17d1deb818daaee8dbcbbcfdcec5074cc114942f6ff084f2d748e"

        self.assertEqual(self.build_key(), expected)

    def test_same_artifact_produces_same_key(self) -> None:
        self.assertEqual(self.build_key(), self.build_key())

    def test_different_artifact_produces_different_key(self) -> None:
        first = self.build_key(artifact_sha256="a" * 64)
        second = self.build_key(artifact_sha256="b" * 64)

        self.assertNotEqual(first, second)

    def test_field_separator_prevents_boundary_collisions(self) -> None:
        first = self.build_key(source_system="abc", dataset="defg")
        second = self.build_key(source_system="abcd", dataset="efg")

        self.assertNotEqual(first, second)

    def test_rejects_invalid_source_name(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "source_system",
        ):
            self.build_key(source_system="ERP FACTORY")

    def test_rejects_invalid_schema_version(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "schema_version",
        ):
            self.build_key(schema_version="v1")

    def test_rejects_invalid_artifact_digest(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "artifact_sha256",
        ):
            self.build_key(artifact_sha256="1234")

    def test_rejects_datetime_as_logical_date(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "logical_date",
        ):
            self.build_key(logical_date=datetime(2026, 9, 5, tzinfo=UTC))
