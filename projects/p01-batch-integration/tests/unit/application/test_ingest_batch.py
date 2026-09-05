"""Behavior tests for the idempotent batch-ingestion service."""

from unittest import TestCase

from factoryflow_batch.application import (
    ArtifactIntegrityError,
    BatchIngestionDisposition,
    BatchRegistryConsistencyError,
    IngestBatchService,
)
from factoryflow_batch.ports import BatchRegistration
from tests.fakes.ingestion import (
    ARTIFACT_SHA256,
    FIXED_BATCH_ID,
    SECOND_BATCH_ID,
    FixedBatchIdGenerator,
    LandingCall,
    StubArtifactInspector,
    StubBatchRegistry,
    StubLandingStore,
    expected_idempotency_key,
    expected_object_key,
    valid_artifact,
    valid_command,
    valid_manifest,
    valid_receipt,
)


class TestIngestBatchService(TestCase):
    """Verify success, retry, race and integrity behavior."""

    def test_creates_lands_and_registers_new_batch(self) -> None:
        command = valid_command()
        inspector = StubArtifactInspector(valid_artifact())
        landing_store = StubLandingStore(valid_receipt())
        registry = StubBatchRegistry()
        batch_ids = FixedBatchIdGenerator()

        service = IngestBatchService(
            inspector=inspector,
            landing_store=landing_store,
            registry=registry,
            batch_ids=batch_ids,
        )

        result = service.execute(command)

        self.assertEqual(
            result.disposition,
            BatchIngestionDisposition.CREATED,
        )
        self.assertEqual(result.manifest.batch_id, FIXED_BATCH_ID)
        self.assertEqual(
            result.manifest.idempotency_key,
            expected_idempotency_key(),
        )
        self.assertEqual(
            inspector.calls,
            [command.source_uri],
        )
        self.assertEqual(
            landing_store.calls,
            [
                LandingCall(
                    source_uri=command.source_uri,
                    object_key=expected_object_key(),
                    expected_sha256=ARTIFACT_SHA256,
                )
            ],
        )
        self.assertEqual(registry.registrations, [result.manifest])
        self.assertEqual(batch_ids.calls, 1)

    def test_returns_existing_batch_without_landing_again(self) -> None:
        command = valid_command()
        existing = valid_manifest()
        inspector = StubArtifactInspector(valid_artifact())
        landing_store = StubLandingStore(valid_receipt())
        registry = StubBatchRegistry(found=existing)
        batch_ids = FixedBatchIdGenerator()

        service = IngestBatchService(
            inspector=inspector,
            landing_store=landing_store,
            registry=registry,
            batch_ids=batch_ids,
        )

        result = service.execute(command)

        self.assertEqual(
            result.disposition,
            BatchIngestionDisposition.DEDUPLICATED,
        )
        self.assertIs(result.manifest, existing)
        self.assertEqual(landing_store.calls, [])
        self.assertEqual(registry.registrations, [])
        self.assertEqual(batch_ids.calls, 0)

    def test_second_execution_is_deduplicated(self) -> None:
        inspector = StubArtifactInspector(valid_artifact())
        landing_store = StubLandingStore(valid_receipt())
        registry = StubBatchRegistry()
        batch_ids = FixedBatchIdGenerator()

        service = IngestBatchService(
            inspector=inspector,
            landing_store=landing_store,
            registry=registry,
            batch_ids=batch_ids,
        )

        first = service.execute(valid_command())
        second = service.execute(valid_command())

        self.assertEqual(first.disposition, BatchIngestionDisposition.CREATED)
        self.assertEqual(
            second.disposition,
            BatchIngestionDisposition.DEDUPLICATED,
        )
        self.assertIs(second.manifest, first.manifest)
        self.assertEqual(len(landing_store.calls), 1)
        self.assertEqual(len(registry.registrations), 1)
        self.assertEqual(batch_ids.calls, 1)

    def test_concurrent_registration_returns_winning_manifest(self) -> None:
        winner = valid_manifest(batch_id=SECOND_BATCH_ID)
        registry = StubBatchRegistry(
            registration=BatchRegistration(
                created=False,
                manifest=winner,
            )
        )
        landing_store = StubLandingStore(valid_receipt())

        service = IngestBatchService(
            inspector=StubArtifactInspector(valid_artifact()),
            landing_store=landing_store,
            registry=registry,
            batch_ids=FixedBatchIdGenerator(),
        )

        result = service.execute(valid_command())

        self.assertEqual(
            result.disposition,
            BatchIngestionDisposition.DEDUPLICATED,
        )
        self.assertIs(result.manifest, winner)
        self.assertEqual(len(landing_store.calls), 1)
        self.assertEqual(len(registry.registrations), 1)

    def test_rejects_landing_checksum_mismatch(self) -> None:
        registry = StubBatchRegistry()
        batch_ids = FixedBatchIdGenerator()
        service = IngestBatchService(
            inspector=StubArtifactInspector(valid_artifact()),
            landing_store=StubLandingStore(valid_receipt(sha256="b" * 64)),
            registry=registry,
            batch_ids=batch_ids,
        )

        with self.assertRaisesRegex(
            ArtifactIntegrityError,
            "SHA-256 differs",
        ):
            service.execute(valid_command())

        self.assertEqual(registry.registrations, [])
        self.assertEqual(batch_ids.calls, 0)

    def test_rejects_landing_size_mismatch(self) -> None:
        registry = StubBatchRegistry()
        batch_ids = FixedBatchIdGenerator()
        service = IngestBatchService(
            inspector=StubArtifactInspector(valid_artifact()),
            landing_store=StubLandingStore(valid_receipt(file_size_bytes=4097)),
            registry=registry,
            batch_ids=batch_ids,
        )

        with self.assertRaisesRegex(
            ArtifactIntegrityError,
            "size differs",
        ):
            service.execute(valid_command())

        self.assertEqual(registry.registrations, [])
        self.assertEqual(batch_ids.calls, 0)

    def test_rejects_inconsistent_lookup_result(self) -> None:
        inconsistent = valid_manifest(artifact_sha256="b" * 64)
        landing_store = StubLandingStore(valid_receipt())
        service = IngestBatchService(
            inspector=StubArtifactInspector(valid_artifact()),
            landing_store=landing_store,
            registry=StubBatchRegistry(found=inconsistent),
            batch_ids=FixedBatchIdGenerator(),
        )

        with self.assertRaisesRegex(
            BatchRegistryConsistencyError,
            "another idempotency key",
        ):
            service.execute(valid_command())

        self.assertEqual(landing_store.calls, [])

    def test_rejects_inconsistent_created_registration(self) -> None:
        different_manifest = valid_manifest(batch_id=SECOND_BATCH_ID)
        registry = StubBatchRegistry(
            registration=BatchRegistration(
                created=True,
                manifest=different_manifest,
            )
        )

        service = IngestBatchService(
            inspector=StubArtifactInspector(valid_artifact()),
            landing_store=StubLandingStore(valid_receipt()),
            registry=registry,
            batch_ids=FixedBatchIdGenerator(),
        )

        with self.assertRaisesRegex(
            BatchRegistryConsistencyError,
            "reported creation",
        ):
            service.execute(valid_command())
