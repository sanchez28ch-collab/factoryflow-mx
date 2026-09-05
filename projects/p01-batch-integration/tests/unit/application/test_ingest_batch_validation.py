"""Defensive validation tests for the batch-ingestion service."""

from dataclasses import replace
from datetime import datetime
from unittest import TestCase

from factoryflow_batch.application import (
    BatchIngestionDisposition,
    BatchIngestionValidationError,
    IngestBatchService,
)
from factoryflow_batch.domain import BatchManifestValidationError
from factoryflow_batch.ports import InspectedArtifact
from tests.fakes.ingestion import (
    FixedBatchIdGenerator,
    StubArtifactInspector,
    StubBatchRegistry,
    StubLandingStore,
    valid_artifact,
    valid_command,
    valid_receipt,
)


class TestIngestBatchValidation(TestCase):
    """Reject invalid inputs before irreversible side effects."""

    @staticmethod
    def _service_for(
        artifact: InspectedArtifact,
    ) -> tuple[
        IngestBatchService,
        StubArtifactInspector,
        StubLandingStore,
        StubBatchRegistry,
        FixedBatchIdGenerator,
    ]:
        inspector = StubArtifactInspector(artifact)
        landing_store = StubLandingStore(valid_receipt())
        registry = StubBatchRegistry()
        batch_ids = FixedBatchIdGenerator()

        service = IngestBatchService(
            inspector=inspector,
            landing_store=landing_store,
            registry=registry,
            batch_ids=batch_ids,
        )
        return (
            service,
            inspector,
            landing_store,
            registry,
            batch_ids,
        )

    def test_rejects_invalid_source_uris_without_inspection(self) -> None:
        cases = (
            (123, "source_uri must be a string"),
            (
                " file:///tmp/orders.csv",
                "surrounding whitespace",
            ),
            (
                "https://example.com/orders.csv",
                "file or gs scheme",
            ),
            (
                "file:///tmp/orders.csv?token=secret",
                "query parameters or fragments",
            ),
            (
                "file:///tmp/orders.csv#fragment",
                "query parameters or fragments",
            ),
            (
                "file://remote-server/tmp/orders.csv",
                "absolute local path",
            ),
            (
                "file:orders.csv",
                "absolute local path",
            ),
            (
                "gs://incoming-bucket",
                "bucket and object path",
            ),
            (
                "gs:///orders.csv",
                "bucket and object path",
            ),
        )

        for source_uri, expected_message in cases:
            with self.subTest(source_uri=source_uri):
                service, inspector, landing, registry, batch_ids = self._service_for(
                    valid_artifact()
                )
                command = replace(
                    valid_command(),
                    source_uri=source_uri,
                )

                with self.assertRaisesRegex(
                    BatchIngestionValidationError,
                    expected_message,
                ):
                    service.execute(command)

                self.assertEqual(inspector.calls, [])
                self.assertEqual(landing.calls, [])
                self.assertEqual(registry.lookups, [])
                self.assertEqual(batch_ids.calls, 0)

    def test_rejects_invalid_extraction_timestamps(self) -> None:
        cases = (
            "2026-09-05T12:00:00Z",
            datetime(2026, 9, 5, 12, 0),
        )

        for extracted_at in cases:
            with self.subTest(extracted_at=extracted_at):
                service, inspector, landing, registry, batch_ids = self._service_for(
                    valid_artifact()
                )
                command = replace(
                    valid_command(),
                    extracted_at=extracted_at,
                )

                with self.assertRaises(BatchIngestionValidationError):
                    service.execute(command)

                self.assertEqual(inspector.calls, [])
                self.assertEqual(landing.calls, [])
                self.assertEqual(registry.lookups, [])
                self.assertEqual(batch_ids.calls, 0)

    def test_rejects_invalid_context_before_inspection(self) -> None:
        service, inspector, landing, registry, batch_ids = self._service_for(valid_artifact())
        command = replace(
            valid_command(),
            source_system="invalid source",
        )

        with self.assertRaises(BatchManifestValidationError):
            service.execute(command)

        self.assertEqual(inspector.calls, [])
        self.assertEqual(landing.calls, [])
        self.assertEqual(registry.lookups, [])
        self.assertEqual(batch_ids.calls, 0)

    def test_rejects_unsafe_inspector_file_names(self) -> None:
        invalid_names = (
            "",
            ".",
            "..",
            "../orders.csv",
            "nested/orders.csv",
            r"nested\orders.csv",
        )

        for source_file_name in invalid_names:
            with self.subTest(source_file_name=source_file_name):
                artifact = replace(
                    valid_artifact(),
                    source_file_name=source_file_name,
                )
                service, inspector, landing, registry, batch_ids = self._service_for(artifact)

                with self.assertRaisesRegex(
                    BatchIngestionValidationError,
                    "unsafe source_file_name",
                ):
                    service.execute(valid_command())

                self.assertEqual(len(inspector.calls), 1)
                self.assertEqual(landing.calls, [])
                self.assertEqual(registry.lookups, [])
                self.assertEqual(batch_ids.calls, 0)

    def test_rejects_invalid_inspector_content_types(self) -> None:
        invalid_content_types = (
            123,
            "textcsv",
            " text/csv",
            "text/csv ",
        )

        for content_type in invalid_content_types:
            with self.subTest(content_type=content_type):
                artifact = replace(
                    valid_artifact(),
                    content_type=content_type,
                )
                service, _, landing, registry, batch_ids = self._service_for(artifact)

                with self.assertRaisesRegex(
                    BatchIngestionValidationError,
                    "invalid content_type",
                ):
                    service.execute(valid_command())

                self.assertEqual(landing.calls, [])
                self.assertEqual(registry.lookups, [])
                self.assertEqual(batch_ids.calls, 0)

    def test_rejects_invalid_inspector_record_counts(self) -> None:
        invalid_counts = (
            True,
            "25",
            -1,
        )

        for record_count in invalid_counts:
            with self.subTest(record_count=record_count):
                artifact = replace(
                    valid_artifact(),
                    record_count=record_count,
                )
                service, _, landing, registry, batch_ids = self._service_for(artifact)

                with self.assertRaisesRegex(
                    BatchIngestionValidationError,
                    "invalid record_count",
                ):
                    service.execute(valid_command())

                self.assertEqual(landing.calls, [])
                self.assertEqual(registry.lookups, [])
                self.assertEqual(batch_ids.calls, 0)

    def test_rejects_invalid_inspector_file_sizes(self) -> None:
        invalid_sizes = (
            True,
            "4096",
            0,
            -1,
        )

        for file_size_bytes in invalid_sizes:
            with self.subTest(file_size_bytes=file_size_bytes):
                artifact = replace(
                    valid_artifact(),
                    file_size_bytes=file_size_bytes,
                )
                service, _, landing, registry, batch_ids = self._service_for(artifact)

                with self.assertRaisesRegex(
                    BatchIngestionValidationError,
                    "invalid file_size_bytes",
                ):
                    service.execute(valid_command())

                self.assertEqual(landing.calls, [])
                self.assertEqual(registry.lookups, [])
                self.assertEqual(batch_ids.calls, 0)

    def test_rejects_invalid_artifact_digest(self) -> None:
        artifact = replace(
            valid_artifact(),
            sha256="NOT-A-SHA256",
        )
        service, _, landing, registry, batch_ids = self._service_for(artifact)

        with self.assertRaises(BatchManifestValidationError):
            service.execute(valid_command())

        self.assertEqual(landing.calls, [])
        self.assertEqual(registry.lookups, [])
        self.assertEqual(batch_ids.calls, 0)

    def test_accepts_gcs_source_uri(self) -> None:
        service, inspector, landing, registry, batch_ids = self._service_for(valid_artifact())
        command = replace(
            valid_command(),
            source_uri="gs://factoryflow-incoming/orders/orders.csv",
        )

        result = service.execute(command)

        self.assertEqual(
            result.disposition,
            BatchIngestionDisposition.CREATED,
        )
        self.assertEqual(inspector.calls, [command.source_uri])
        self.assertEqual(len(landing.calls), 1)
        self.assertEqual(len(registry.registrations), 1)
        self.assertEqual(batch_ids.calls, 1)
