"""Unit tests for BatchManifest JSON serialization."""

import json
from pathlib import Path
from typing import cast
from unittest import TestCase

from factoryflow_batch.adapters import (
    batch_manifest_from_mapping,
    dumps_batch_manifest,
    loads_batch_manifest,
)
from factoryflow_batch.domain import BatchManifestValidationError

_REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
_FIXTURES = _REPOSITORY_ROOT / "projects/p01-batch-integration/tests/fixtures"


def _fixture_mapping(relative_path: str) -> dict[str, object]:
    document = (_FIXTURES / relative_path).read_text(encoding="utf-8")
    payload: object = json.loads(document)

    if not isinstance(payload, dict):
        raise AssertionError("Fixture must contain a JSON object.")

    return cast(dict[str, object], payload)


class TestManifestJsonAdapter(TestCase):
    """Verify safe and deterministic JSON conversion."""

    def test_deserializes_valid_fixture(self) -> None:
        payload = _fixture_mapping("valid/batch-manifest-v1.json")

        manifest = batch_manifest_from_mapping(payload)

        self.assertEqual(manifest.source_system, "erp_factory")
        self.assertEqual(manifest.dataset, "production_orders")
        self.assertEqual(manifest.record_count, 1250)

    def test_round_trip_preserves_manifest(self) -> None:
        payload = _fixture_mapping("valid/batch-manifest-v1.json")
        original = batch_manifest_from_mapping(payload)

        reconstructed = loads_batch_manifest(dumps_batch_manifest(original))

        self.assertEqual(reconstructed, original)

    def test_serialization_is_deterministic(self) -> None:
        payload = _fixture_mapping("valid/batch-manifest-v1.json")
        manifest = batch_manifest_from_mapping(payload)

        first = dumps_batch_manifest(manifest)
        second = dumps_batch_manifest(manifest)

        self.assertEqual(first, second)
        self.assertTrue(first.endswith("\n"))

    def test_rejects_missing_required_field(self) -> None:
        payload = _fixture_mapping("valid/batch-manifest-v1.json")
        del payload["dataset"]

        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "missing required fields: dataset",
        ):
            batch_manifest_from_mapping(payload)

    def test_rejects_unexpected_field(self) -> None:
        payload = _fixture_mapping("valid/batch-manifest-v1.json")
        payload["password"] = "must-not-be-accepted"

        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "unexpected fields: password",
        ):
            batch_manifest_from_mapping(payload)

    def test_rejects_malformed_json(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "not valid JSON",
        ):
            loads_batch_manifest('{"batch_id":')

    def test_rejects_json_array(self) -> None:
        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "must contain a JSON object",
        ):
            loads_batch_manifest("[]")

    def test_rejects_invalid_uuid(self) -> None:
        payload = _fixture_mapping("valid/batch-manifest-v1.json")
        payload["batch_id"] = "not-a-uuid"

        with self.assertRaisesRegex(
            BatchManifestValidationError,
            "batch_id must be a valid UUID",
        ):
            batch_manifest_from_mapping(payload)

    def test_rejects_complete_invalid_fixture(self) -> None:
        payload = _fixture_mapping("invalid/batch-manifest-v1-invalid.json")

        with self.assertRaises(BatchManifestValidationError):
            batch_manifest_from_mapping(payload)
