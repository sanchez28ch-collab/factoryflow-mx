"""Unit tests for the immutable BatchManifest domain object."""

from dataclasses import FrozenInstanceError, replace
from datetime import UTC, date, datetime, timedelta, timezone
from unittest import TestCase
from uuid import UUID

from factoryflow_batch.domain import (
    BatchManifest,
    BatchManifestValidationError,
)


class TestBatchManifest(TestCase):
    """Verify construction, normalization and domain invariants."""

    def build_manifest(self, **overrides: object) -> BatchManifest:
        parameters = {
            "source_system": "erp_factory",
            "dataset": "production_orders",
            "schema_version": "1.0.0",
            "logical_date": date(2026, 9, 5),
            "source_file_name": "production_orders_20260905.csv",
            "landing_uri": "gs://factoryflow-dev-raw/orders/file.csv",
            "content_type": "text/csv",
            "extracted_at": datetime(2026, 9, 5, 12, 0, tzinfo=UTC),
            "landed_at": datetime(2026, 9, 5, 12, 0, 5, tzinfo=UTC),
            "record_count": 1250,
            "file_size_bytes": 98432,
            "sha256": "a" * 64,
            "batch_id": UUID("018f4b26-7c11-7a32-8d26-4beef4c28f21"),
        }
        parameters.update(overrides)
        return BatchManifest.create(**parameters)  # type: ignore[arg-type]

    def test_creates_valid_manifest(self) -> None:
        manifest = self.build_manifest()

        self.assertEqual(manifest.manifest_version, "1.0.0")
        self.assertEqual(manifest.record_count, 1250)
        self.assertEqual(
            manifest.idempotency_key,
            ("01912fd29cd17d1deb818daaee8dbcbbcfdcec5074cc114942f6ff084f2d748e"),
        )

    def test_is_immutable(self) -> None:
        manifest = self.build_manifest()

        with self.assertRaises(FrozenInstanceError):
            manifest.dataset = "modified"

    def test_normalizes_datetimes_to_utc(self) -> None:
        mexico_offset = timezone(timedelta(hours=-6))
        manifest = self.build_manifest(
            extracted_at=datetime(
                2026,
                9,
                5,
                6,
                0,
                tzinfo=mexico_offset,
            ),
            landed_at=datetime(
                2026,
                9,
                5,
                6,
                0,
                5,
                tzinfo=mexico_offset,
            ),
        )

        self.assertIs(manifest.extracted_at.tzinfo, UTC)
        self.assertEqual(manifest.extracted_at.hour, 12)

    def test_rejects_landing_before_extraction(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "landed_at cannot be earlier",
        ):
            self.build_manifest(
                landed_at=datetime(
                    2026,
                    9,
                    5,
                    11,
                    59,
                    tzinfo=UTC,
                )
            )

    def test_rejects_mismatched_idempotency_key(self) -> None:
        manifest = self.build_manifest()

        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "idempotency_key does not match",
        ):
            replace(manifest, idempotency_key="b" * 64)

    def test_rejects_unsupported_content_type(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "content_type",
        ):
            self.build_manifest(content_type="application/octet-stream")

    def test_rejects_invalid_landing_uri(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "landing_uri",
        ):
            self.build_manifest(landing_uri="https://example.com/orders.csv")

    def test_rejects_boolean_record_count(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "record_count",
        ):
            self.build_manifest(record_count=True)

    def test_rejects_zero_file_size(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "file_size_bytes",
        ):
            self.build_manifest(file_size_bytes=0)

    def test_rejects_unsafe_source_file_name(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "source_file_name",
        ):
            self.build_manifest(source_file_name="../production_orders.csv")
