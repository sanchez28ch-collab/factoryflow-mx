"""Unit tests for deterministic raw-zone object keys."""

from datetime import date
from unittest import TestCase

from factoryflow_batch.application import build_landing_object_key


class TestLandingObjectKey(TestCase):
    """Verify partition layout and safe file-name handling."""

    def test_builds_partitioned_key_and_normalizes_suffix(self) -> None:
        result = build_landing_object_key(
            source_system="erp",
            dataset="production_orders",
            logical_date=date(2026, 9, 5),
            schema_version="1.0.0",
            idempotency_key="a" * 64,
            source_file_name="orders.CSV",
        )

        self.assertEqual(
            result,
            "raw/source_system=erp/dataset=production_orders/"
            "logical_date=2026-09-05/schema_version=1.0.0/"
            f"{'a' * 64}.csv",
        )

    def test_supports_extensionless_artifact(self) -> None:
        result = build_landing_object_key(
            source_system="erp",
            dataset="production_orders",
            logical_date=date(2026, 9, 5),
            schema_version="1.0.0",
            idempotency_key="b" * 64,
            source_file_name="orders",
        )

        self.assertTrue(result.endswith("b" * 64))

    def test_rejects_unsafe_source_file_names(self) -> None:
        invalid_names = (
            "",
            ".",
            "..",
            "../orders.csv",
            "nested/orders.csv",
            r"nested\orders.csv",
        )

        for invalid_name in invalid_names:
            with (
                self.subTest(invalid_name=invalid_name),
                self.assertRaisesRegex(
                    ValueError,
                    "source_file_name must be a safe base name",
                ),
            ):
                build_landing_object_key(
                    source_system="erp",
                    dataset="production_orders",
                    logical_date=date(2026, 9, 5),
                    schema_version="1.0.0",
                    idempotency_key="c" * 64,
                    source_file_name=invalid_name,
                )
