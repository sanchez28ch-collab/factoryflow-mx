"""Unit tests for reusable batch-domain validators."""

from datetime import date, datetime
from unittest import TestCase

from factoryflow_batch.domain import BatchManifestValidationError
from factoryflow_batch.domain.validation import (
    validated_file_name,
    validated_landing_uri,
    validated_logical_date,
    validated_name,
    validated_non_negative_integer,
    validated_utc_datetime,
)


class TestDomainValidation(TestCase):
    """Exercise defensive branches used by all batch contracts."""

    def test_rejects_non_string_name(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "source_system must be a string",
        ):
            validated_name(123, "source_system")

    def test_rejects_short_name(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "length must be between 3 and 63",
        ):
            validated_name("ab", "source_system")

    def test_rejects_non_date_logical_date(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "logical_date must be a date",
        ):
            validated_logical_date("2026-09-05")

    def test_rejects_non_datetime_timestamp(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "extracted_at must be a datetime",
        ):
            validated_utc_datetime(
                "2026-09-05T12:00:00Z",
                "extracted_at",
            )

    def test_rejects_naive_datetime(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "must include timezone information",
        ):
            validated_utc_datetime(
                datetime(2026, 9, 5, 12, 0),
                "extracted_at",
            )

    def test_rejects_empty_file_name(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "length must be between 1 and 255",
        ):
            validated_file_name("")

    def test_rejects_uri_with_query_parameters(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "must not contain a query or fragment",
        ):
            validated_landing_uri("gs://factoryflow-raw/orders.csv?token=secret")

    def test_rejects_gcs_uri_without_object_path(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "must contain a bucket and object path",
        ):
            validated_landing_uri("gs://factoryflow-raw/")

    def test_rejects_file_uri_with_remote_host(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "must be an absolute local file URI",
        ):
            validated_landing_uri("file://remote-host/tmp/orders.csv")

    def test_accepts_absolute_local_file_uri(self) -> None:
        uri = "file:///tmp/factoryflow/orders.csv"

        self.assertEqual(validated_landing_uri(uri), uri)

    def test_rejects_non_integer_count(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "record_count must be an integer",
        ):
            validated_non_negative_integer(
                "1250",
                "record_count",
            )

    def test_rejects_negative_count(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "record_count must not be negative",
        ):
            validated_non_negative_integer(
                -1,
                "record_count",
            )

    def test_accepts_zero_records(self) -> None:
        self.assertEqual(
            validated_non_negative_integer(0, "record_count"),
            0,
        )

    def test_accepts_valid_logical_date(self) -> None:
        logical_date = date(2026, 9, 5)

        self.assertEqual(
            validated_logical_date(logical_date),
            logical_date,
        )
