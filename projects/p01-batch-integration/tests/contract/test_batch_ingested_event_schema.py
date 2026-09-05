"""Contract tests for the versioned batch-ingested event."""

import json
from copy import deepcopy
from pathlib import Path
from unittest import TestCase

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource


class TestBatchIngestedEventSchema(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        repository_root = Path(__file__).resolve().parents[4]
        project_root = Path(__file__).resolve().parents[2]

        event_schema_path = (
            repository_root
            / "shared"
            / "contracts"
            / "jsonschema"
            / "batch"
            / "batch-ingested-v1.schema.json"
        )
        manifest_schema_path = (
            repository_root
            / "shared"
            / "contracts"
            / "jsonschema"
            / "batch"
            / "batch-manifest-v1.schema.json"
        )
        manifest_fixture_path = (
            project_root / "tests" / "fixtures" / "valid" / "batch-manifest-v1.json"
        )

        cls.event_schema = json.loads(event_schema_path.read_text(encoding="utf-8"))
        cls.manifest_schema = json.loads(manifest_schema_path.read_text(encoding="utf-8"))
        cls.manifest = json.loads(manifest_fixture_path.read_text(encoding="utf-8"))

        resources = (
            (
                cls.manifest_schema["$id"],
                Resource.from_contents(cls.manifest_schema),
            ),
            (
                cls.event_schema["$id"],
                Resource.from_contents(cls.event_schema),
            ),
        )
        registry = Registry().with_resources(resources)

        cls.validator = Draft202012Validator(
            cls.event_schema,
            registry=registry,
            format_checker=FormatChecker(),
        )

    def _valid_event(self) -> dict[str, object]:
        return {
            "event_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
            "event_type": "factoryflow.batch.ingested",
            "event_version": 1,
            "occurred_at": self.manifest["landed_at"],
            "aggregate_type": "batch",
            "aggregate_id": self.manifest["batch_id"],
            "data": deepcopy(self.manifest),
        }

    def test_schema_is_valid_draft_2020_12(self) -> None:
        Draft202012Validator.check_schema(self.event_schema)

        self.assertEqual(
            self.event_schema["$schema"],
            "https://json-schema.org/draft/2020-12/schema",
        )
        self.assertEqual(
            self.event_schema["$id"],
            "urn:factoryflow:events:batch-ingested:1.0.0",
        )

    def test_accepts_valid_event_with_manifest_contract(self) -> None:
        event = self._valid_event()

        errors = sorted(
            self.validator.iter_errors(event),
            key=lambda error: list(error.absolute_path),
        )

        self.assertEqual(errors, [])

    def test_rejects_invalid_event_envelope(self) -> None:
        scenarios = {
            "unknown property": {
                **self._valid_event(),
                "unexpected": True,
            },
            "wrong event type": {
                **self._valid_event(),
                "event_type": "factoryflow.batch.failed",
            },
            "wrong event version": {
                **self._valid_event(),
                "event_version": 2,
            },
            "invalid event id": {
                **self._valid_event(),
                "event_id": "not-a-uuid",
            },
            "invalid aggregate id": {
                **self._valid_event(),
                "aggregate_id": "not-a-uuid",
            },
            "invalid timestamp": {
                **self._valid_event(),
                "occurred_at": "2026-09-05 10:00:00",
            },
        }

        for name, event in scenarios.items():
            with self.subTest(scenario=name):
                errors = list(self.validator.iter_errors(event))
                self.assertGreater(len(errors), 0)

    def test_rejects_manifest_that_breaks_nested_contract(self) -> None:
        event = self._valid_event()
        manifest = deepcopy(self.manifest)
        manifest["sha256"] = "invalid"
        event["data"] = manifest

        errors = list(self.validator.iter_errors(event))

        self.assertGreater(len(errors), 0)

    def test_aggregate_id_matches_nested_batch_id(self) -> None:
        event = self._valid_event()

        self.assertEqual(
            event["aggregate_id"],
            event["data"]["batch_id"],
        )
