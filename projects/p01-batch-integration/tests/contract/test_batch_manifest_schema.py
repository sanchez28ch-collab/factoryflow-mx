"""Contract tests for Batch Manifest JSON Schema v1."""

import json
from pathlib import Path
from typing import cast
from unittest import TestCase

from jsonschema import Draft202012Validator, FormatChecker

_REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
_SCHEMA_PATH = _REPOSITORY_ROOT / "shared/contracts/jsonschema/batch/batch-manifest-v1.schema.json"
_FIXTURES = _REPOSITORY_ROOT / "projects/p01-batch-integration/tests/fixtures"


def _load_object(path: Path) -> dict[str, object]:
    payload: object = json.loads(path.read_text(encoding="utf-8"))

    if not isinstance(payload, dict):
        raise AssertionError(f"{path} must contain a JSON object.")

    return cast(dict[str, object], payload)


class TestBatchManifestSchema(TestCase):
    """Verify syntax, positive examples and intentional violations."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.schema = _load_object(_SCHEMA_PATH)
        cls.validator = Draft202012Validator(
            cls.schema,
            format_checker=FormatChecker(),
        )

    def test_schema_is_valid_draft_2020_12(self) -> None:
        Draft202012Validator.check_schema(self.schema)

    def test_contract_identifier_is_versioned(self) -> None:
        self.assertEqual(
            self.schema["$id"],
            "urn:factoryflow:contracts:batch-manifest:1.0.0",
        )
        self.assertEqual(
            self.schema["$schema"],
            "https://json-schema.org/draft/2020-12/schema",
        )

    def test_valid_fixture_satisfies_contract(self) -> None:
        payload = _load_object(_FIXTURES / "valid/batch-manifest-v1.json")

        errors = list(self.validator.iter_errors(payload))

        self.assertEqual(errors, [])

    def test_schema_requires_exactly_defined_properties(self) -> None:
        required = set(cast(list[str], self.schema["required"]))
        properties = set(cast(dict[str, object], self.schema["properties"]))

        self.assertEqual(required, properties)
        self.assertFalse(self.schema["additionalProperties"])

    def test_invalid_fixture_exercises_expected_violations(self) -> None:
        payload = _load_object(_FIXTURES / "invalid/batch-manifest-v1-invalid.json")

        errors = list(self.validator.iter_errors(payload))
        failing_fields = {str(error.absolute_path[0]) for error in errors if error.absolute_path}

        expected_failing_fields = {
            "manifest_version",
            "batch_id",
            "idempotency_key",
            "source_system",
            "schema_version",
            "logical_date",
            "source_file_name",
            "landing_uri",
            "content_type",
            "extracted_at",
            "landed_at",
            "record_count",
            "file_size_bytes",
            "sha256",
        }

        self.assertTrue(
            expected_failing_fields.issubset(failing_fields),
            expected_failing_fields - failing_fields,
        )
        self.assertTrue(
            any(not error.absolute_path and "dataset" in error.message for error in errors)
        )
        self.assertTrue(
            any(not error.absolute_path and "unexpected_field" in error.message for error in errors)
        )
