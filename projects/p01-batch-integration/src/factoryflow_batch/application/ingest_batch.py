"""Idempotent application service for landing and registering batch files."""

from datetime import datetime
from urllib.parse import urlsplit

from factoryflow_batch.application.exceptions import (
    ArtifactIntegrityError,
    BatchIngestionValidationError,
    BatchRegistryConsistencyError,
)
from factoryflow_batch.application.landing_keys import build_landing_object_key
from factoryflow_batch.application.models import (
    BatchIngestionDisposition,
    BatchIngestionResult,
    IngestBatchCommand,
)
from factoryflow_batch.domain import (
    BatchManifest,
    build_batch_idempotency_key,
)
from factoryflow_batch.ports import (
    ArtifactInspector,
    BatchIdGenerator,
    BatchRegistry,
    InspectedArtifact,
    LandingReceipt,
    LandingStore,
)

_MANIFEST_VERSION = "1.0.0"
_VALIDATION_SHA256 = "0" * 64


class IngestBatchService:
    """Coordinate one ingestion without depending on infrastructure."""

    def __init__(
        self,
        *,
        inspector: ArtifactInspector,
        landing_store: LandingStore,
        registry: BatchRegistry,
        batch_ids: BatchIdGenerator,
    ) -> None:
        self._inspector = inspector
        self._landing_store = landing_store
        self._registry = registry
        self._batch_ids = batch_ids

    def execute(self, command: IngestBatchCommand) -> BatchIngestionResult:
        """Inspect, deduplicate, land and atomically register one artifact."""

        self._validate_command(command)

        artifact = self._inspector.inspect(command.source_uri)
        idempotency_key = build_batch_idempotency_key(
            source_system=command.source_system,
            dataset=command.dataset,
            logical_date=command.logical_date,
            schema_version=command.schema_version,
            artifact_sha256=artifact.sha256,
        )
        self._validate_artifact(artifact)

        existing = self._registry.find_by_idempotency_key(idempotency_key)
        if existing is not None:
            self._verify_registered_manifest(
                existing=existing,
                expected_idempotency_key=idempotency_key,
            )
            return BatchIngestionResult(
                disposition=BatchIngestionDisposition.DEDUPLICATED,
                manifest=existing,
            )

        object_key = build_landing_object_key(
            source_system=command.source_system,
            dataset=command.dataset,
            logical_date=command.logical_date,
            schema_version=command.schema_version,
            idempotency_key=idempotency_key,
            source_file_name=artifact.source_file_name,
        )

        receipt = self._landing_store.land(
            source_uri=command.source_uri,
            object_key=object_key,
            expected_sha256=artifact.sha256,
        )
        self._verify_landing_receipt(
            artifact=artifact,
            receipt=receipt,
        )

        candidate = BatchManifest(
            manifest_version=_MANIFEST_VERSION,
            batch_id=self._batch_ids.new_batch_id(),
            idempotency_key=idempotency_key,
            source_system=command.source_system,
            dataset=command.dataset,
            schema_version=command.schema_version,
            logical_date=command.logical_date,
            source_file_name=artifact.source_file_name,
            landing_uri=receipt.landing_uri,
            content_type=artifact.content_type,
            extracted_at=command.extracted_at,
            landed_at=receipt.landed_at,
            record_count=artifact.record_count,
            file_size_bytes=receipt.file_size_bytes,
            sha256=receipt.sha256,
        )

        registration = self._registry.register_if_absent(candidate)
        self._verify_registered_manifest(
            existing=registration.manifest,
            expected_idempotency_key=idempotency_key,
        )

        if registration.created and registration.manifest != candidate:
            raise BatchRegistryConsistencyError(
                "Registry reported creation but returned a different manifest."
            )

        disposition = (
            BatchIngestionDisposition.CREATED
            if registration.created
            else BatchIngestionDisposition.DEDUPLICATED
        )
        return BatchIngestionResult(
            disposition=disposition,
            manifest=registration.manifest,
        )

    @staticmethod
    def _validate_command(command: IngestBatchCommand) -> None:
        if not isinstance(command.source_uri, str):
            raise BatchIngestionValidationError("source_uri must be a string.")

        if command.source_uri != command.source_uri.strip():
            raise BatchIngestionValidationError("source_uri cannot contain surrounding whitespace.")

        parsed_uri = urlsplit(command.source_uri)
        if parsed_uri.scheme not in {"file", "gs"}:
            raise BatchIngestionValidationError("source_uri must use the file or gs scheme.")

        if parsed_uri.query or parsed_uri.fragment:
            raise BatchIngestionValidationError(
                "source_uri cannot contain query parameters or fragments."
            )

        if parsed_uri.scheme == "file":
            if parsed_uri.netloc or not parsed_uri.path.startswith("/"):
                raise BatchIngestionValidationError(
                    "file source_uri must reference an absolute local path."
                )
        elif not parsed_uri.netloc or not parsed_uri.path.strip("/"):
            raise BatchIngestionValidationError(
                "gs source_uri must include a bucket and object path."
            )

        if not isinstance(command.extracted_at, datetime):
            raise BatchIngestionValidationError("extracted_at must be a datetime.")

        if command.extracted_at.tzinfo is None or command.extracted_at.utcoffset() is None:
            raise BatchIngestionValidationError("extracted_at must include a UTC offset.")

        build_batch_idempotency_key(
            source_system=command.source_system,
            dataset=command.dataset,
            logical_date=command.logical_date,
            schema_version=command.schema_version,
            artifact_sha256=_VALIDATION_SHA256,
        )

    @staticmethod
    def _validate_artifact(artifact: InspectedArtifact) -> None:
        if (
            not isinstance(artifact.source_file_name, str)
            or not artifact.source_file_name
            or artifact.source_file_name in {".", ".."}
            or "/" in artifact.source_file_name
            or "\\" in artifact.source_file_name
        ):
            raise BatchIngestionValidationError("Inspector returned an unsafe source_file_name.")

        if (
            not isinstance(artifact.content_type, str)
            or "/" not in artifact.content_type
            or artifact.content_type != artifact.content_type.strip()
        ):
            raise BatchIngestionValidationError("Inspector returned an invalid content_type.")

        if (
            isinstance(artifact.record_count, bool)
            or not isinstance(artifact.record_count, int)
            or artifact.record_count < 0
        ):
            raise BatchIngestionValidationError("Inspector returned an invalid record_count.")

        if (
            isinstance(artifact.file_size_bytes, bool)
            or not isinstance(artifact.file_size_bytes, int)
            or artifact.file_size_bytes <= 0
        ):
            raise BatchIngestionValidationError("Inspector returned an invalid file_size_bytes.")

    @staticmethod
    def _verify_landing_receipt(
        *,
        artifact: InspectedArtifact,
        receipt: LandingReceipt,
    ) -> None:
        if receipt.sha256 != artifact.sha256:
            raise ArtifactIntegrityError(
                "Landed artifact SHA-256 differs from the inspected source."
            )

        if receipt.file_size_bytes != artifact.file_size_bytes:
            raise ArtifactIntegrityError("Landed artifact size differs from the inspected source.")

    @staticmethod
    def _verify_registered_manifest(
        *,
        existing: BatchManifest,
        expected_idempotency_key: str,
    ) -> None:
        if existing.idempotency_key != expected_idempotency_key:
            raise BatchRegistryConsistencyError(
                "Registry returned a manifest for another idempotency key."
            )
