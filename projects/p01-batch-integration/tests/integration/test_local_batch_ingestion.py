"""End-to-end local ingestion through real CSV and landing adapters."""

from datetime import UTC, date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from urllib.parse import unquote, urlsplit

from factoryflow_batch.adapters import (
    LocalCsvArtifactInspector,
    LocalFilesystemLandingStore,
)
from factoryflow_batch.application import (
    BatchIngestionDisposition,
    IngestBatchCommand,
    IngestBatchService,
)
from tests.fakes.ingestion import (
    FixedBatchIdGenerator,
    StubBatchRegistry,
)


class TestLocalBatchIngestion(TestCase):
    """Execute the application service against real local files."""

    def test_ingests_real_csv_and_deduplicates_retry(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            base_root = Path(temporary_directory)
            incoming_root = base_root / "incoming"
            landing_root = base_root / "landing"
            incoming_root.mkdir()

            payload = b"order_id,product,quantity\n1001,panel,2\n1002,base,1\n"
            source = incoming_root / "production_orders.csv"
            source.write_bytes(payload)

            registry = StubBatchRegistry()
            batch_ids = FixedBatchIdGenerator()
            service = IngestBatchService(
                inspector=LocalCsvArtifactInspector(
                    incoming_root,
                    chunk_size=7,
                ),
                landing_store=LocalFilesystemLandingStore(
                    source_root=incoming_root,
                    landing_root=landing_root,
                    chunk_size=7,
                ),
                registry=registry,
                batch_ids=batch_ids,
            )
            command = IngestBatchCommand(
                source_system="erp",
                dataset="production_orders",
                schema_version="1.0.0",
                logical_date=date(2026, 9, 5),
                source_uri=source.as_uri(),
                extracted_at=datetime(
                    2020,
                    1,
                    1,
                    0,
                    0,
                    tzinfo=UTC,
                ),
            )

            first = service.execute(command)
            second = service.execute(command)

            self.assertEqual(
                first.disposition,
                BatchIngestionDisposition.CREATED,
            )
            self.assertEqual(
                second.disposition,
                BatchIngestionDisposition.DEDUPLICATED,
            )
            self.assertIs(second.manifest, first.manifest)
            self.assertEqual(first.manifest.record_count, 2)
            self.assertEqual(first.manifest.file_size_bytes, len(payload))

            parsed_uri = urlsplit(first.manifest.landing_uri)
            landed_path = Path(unquote(parsed_uri.path))

            self.assertEqual(landed_path.read_bytes(), payload)
            self.assertTrue(landed_path.is_relative_to(landing_root.resolve()))
            self.assertEqual(len(registry.registrations), 1)
            self.assertEqual(batch_ids.calls, 1)

            landed_files = [path for path in landing_root.rglob("*") if path.is_file()]
            self.assertEqual(landed_files, [landed_path])
