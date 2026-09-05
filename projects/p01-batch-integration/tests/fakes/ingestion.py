"""Deterministic test doubles for the batch-ingestion application service."""

from dataclasses import dataclass
from datetime import UTC, date, datetime
from uuid import UUID

from factoryflow_batch.application import (
    IngestBatchCommand,
    build_landing_object_key,
)
from factoryflow_batch.domain import (
    BatchManifest,
    build_batch_idempotency_key,
)
from factoryflow_batch.ports import (
    BatchRegistration,
    InspectedArtifact,
    LandingReceipt,
)

ARTIFACT_SHA256 = "a" * 64
FIXED_BATCH_ID = UUID("9b32a4b1-2c3d-4e5f-8a6b-7c8d9e0f1234")
SECOND_BATCH_ID = UUID("8c21b3a0-1d2e-4f5a-9b6c-7d8e9f0a1234")


def valid_command() -> IngestBatchCommand:
    """Return a deterministic ingestion command."""

    return IngestBatchCommand(
        source_system="erp",
        dataset="production_orders",
        schema_version="1.0.0",
        logical_date=date(2026, 9, 5),
        source_uri="file:///tmp/production_orders_20260905.csv",
        extracted_at=datetime(2026, 9, 5, 12, 0, tzinfo=UTC),
    )


def valid_artifact() -> InspectedArtifact:
    """Return deterministic inspected metadata."""

    return InspectedArtifact(
        source_file_name="production_orders_20260905.csv",
        content_type="text/csv",
        record_count=25,
        file_size_bytes=4096,
        sha256=ARTIFACT_SHA256,
    )


def expected_idempotency_key(
    artifact_sha256: str = ARTIFACT_SHA256,
) -> str:
    """Return the idempotency key for the default command."""

    command = valid_command()
    return build_batch_idempotency_key(
        source_system=command.source_system,
        dataset=command.dataset,
        logical_date=command.logical_date,
        schema_version=command.schema_version,
        artifact_sha256=artifact_sha256,
    )


def expected_object_key() -> str:
    """Return the deterministic landing object key."""

    command = valid_command()
    artifact = valid_artifact()
    return build_landing_object_key(
        source_system=command.source_system,
        dataset=command.dataset,
        logical_date=command.logical_date,
        schema_version=command.schema_version,
        idempotency_key=expected_idempotency_key(),
        source_file_name=artifact.source_file_name,
    )


def valid_receipt(
    *,
    sha256: str = ARTIFACT_SHA256,
    file_size_bytes: int = 4096,
) -> LandingReceipt:
    """Return a deterministic landing receipt."""

    return LandingReceipt(
        landing_uri=f"gs://factoryflow-dev-raw/{expected_object_key()}",
        landed_at=datetime(2026, 9, 5, 12, 5, tzinfo=UTC),
        file_size_bytes=file_size_bytes,
        sha256=sha256,
    )


def valid_manifest(
    *,
    batch_id: UUID = FIXED_BATCH_ID,
    artifact_sha256: str = ARTIFACT_SHA256,
) -> BatchManifest:
    """Return a valid manifest for registry scenarios."""

    command = valid_command()
    artifact = valid_artifact()
    idempotency_key = expected_idempotency_key(artifact_sha256)

    return BatchManifest(
        manifest_version="1.0.0",
        batch_id=batch_id,
        idempotency_key=idempotency_key,
        source_system=command.source_system,
        dataset=command.dataset,
        schema_version=command.schema_version,
        logical_date=command.logical_date,
        source_file_name=artifact.source_file_name,
        landing_uri=f"gs://factoryflow-dev-raw/{expected_object_key()}",
        content_type=artifact.content_type,
        extracted_at=command.extracted_at,
        landed_at=datetime(2026, 9, 5, 12, 5, tzinfo=UTC),
        record_count=artifact.record_count,
        file_size_bytes=artifact.file_size_bytes,
        sha256=artifact_sha256,
    )


class StubArtifactInspector:
    """Return configured metadata and record calls."""

    def __init__(self, artifact: InspectedArtifact) -> None:
        self.artifact = artifact
        self.calls: list[str] = []

    def inspect(self, source_uri: str, /) -> InspectedArtifact:
        self.calls.append(source_uri)
        return self.artifact


@dataclass(frozen=True, slots=True)
class LandingCall:
    """Arguments captured by the landing-store double."""

    source_uri: str
    object_key: str
    expected_sha256: str


class StubLandingStore:
    """Return a configured receipt and record calls."""

    def __init__(self, receipt: LandingReceipt) -> None:
        self.receipt = receipt
        self.calls: list[LandingCall] = []

    def land(
        self,
        *,
        source_uri: str,
        object_key: str,
        expected_sha256: str,
    ) -> LandingReceipt:
        self.calls.append(
            LandingCall(
                source_uri=source_uri,
                object_key=object_key,
                expected_sha256=expected_sha256,
            )
        )
        return self.receipt


class StubBatchRegistry:
    """Model lookup, creation and concurrent duplicate outcomes."""

    def __init__(
        self,
        *,
        found: BatchManifest | None = None,
        registration: BatchRegistration | None = None,
    ) -> None:
        self.found = found
        self.registration = registration
        self.lookups: list[str] = []
        self.registrations: list[BatchManifest] = []

    def find_by_idempotency_key(
        self,
        idempotency_key: str,
        /,
    ) -> BatchManifest | None:
        self.lookups.append(idempotency_key)
        return self.found

    def register_if_absent(
        self,
        manifest: BatchManifest,
        /,
    ) -> BatchRegistration:
        self.registrations.append(manifest)

        if self.registration is not None:
            return self.registration

        self.found = manifest
        return BatchRegistration(created=True, manifest=manifest)


class FixedBatchIdGenerator:
    """Return one deterministic UUID and record usage."""

    def __init__(self, value: UUID = FIXED_BATCH_ID) -> None:
        self.value = value
        self.calls = 0

    def new_batch_id(self) -> UUID:
        self.calls += 1
        return self.value
