"""Immutable domain model for a landed batch artifact."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import ClassVar
from uuid import UUID, uuid4

from factoryflow_batch.domain.exceptions import BatchManifestValidationError
from factoryflow_batch.domain.idempotency import (
    build_batch_idempotency_key,
)
from factoryflow_batch.domain.validation import (
    validated_content_type,
    validated_file_name,
    validated_landing_uri,
    validated_logical_date,
    validated_name,
    validated_non_negative_integer,
    validated_positive_integer,
    validated_sha256,
    validated_utc_datetime,
    validated_version,
)


@dataclass(frozen=True, slots=True)
class BatchManifest:
    """Technical identity and immutable metadata of one landed artifact."""

    manifest_version: str
    batch_id: UUID
    idempotency_key: str
    source_system: str
    dataset: str
    schema_version: str
    logical_date: date
    source_file_name: str
    landing_uri: str
    content_type: str
    extracted_at: datetime
    landed_at: datetime
    record_count: int
    file_size_bytes: int
    sha256: str

    CURRENT_VERSION: ClassVar[str] = "1.0.0"

    def __post_init__(self) -> None:
        valid_manifest_version = validated_version(
            self.manifest_version,
            "manifest_version",
        )
        if valid_manifest_version != self.CURRENT_VERSION:
            raise BatchManifestValidationError("manifest_version must be 1.0.0.")

        if not isinstance(self.batch_id, UUID) or self.batch_id.int == 0:
            raise BatchManifestValidationError("batch_id must be a non-zero UUID.")

        valid_source = validated_name(
            self.source_system,
            "source_system",
        )
        valid_dataset = validated_name(
            self.dataset,
            "dataset",
        )
        valid_schema_version = validated_version(
            self.schema_version,
            "schema_version",
        )
        valid_logical_date = validated_logical_date(self.logical_date)
        valid_artifact_sha256 = validated_sha256(
            self.sha256,
            "sha256",
        )

        validated_sha256(
            self.idempotency_key,
            "idempotency_key",
        )
        validated_file_name(self.source_file_name)
        validated_landing_uri(self.landing_uri)
        validated_content_type(self.content_type)
        validated_non_negative_integer(
            self.record_count,
            "record_count",
        )
        validated_positive_integer(
            self.file_size_bytes,
            "file_size_bytes",
        )

        valid_extracted_at = validated_utc_datetime(
            self.extracted_at,
            "extracted_at",
        )
        valid_landed_at = validated_utc_datetime(
            self.landed_at,
            "landed_at",
        )

        if valid_landed_at < valid_extracted_at:
            raise BatchManifestValidationError("landed_at cannot be earlier than extracted_at.")

        expected_idempotency_key = build_batch_idempotency_key(
            source_system=valid_source,
            dataset=valid_dataset,
            logical_date=valid_logical_date,
            schema_version=valid_schema_version,
            artifact_sha256=valid_artifact_sha256,
        )
        if self.idempotency_key != expected_idempotency_key:
            raise BatchManifestValidationError(
                "idempotency_key does not match the artifact identity."
            )

        object.__setattr__(
            self,
            "extracted_at",
            valid_extracted_at,
        )
        object.__setattr__(
            self,
            "landed_at",
            valid_landed_at,
        )

    @classmethod
    def create(
        cls,
        *,
        source_system: str,
        dataset: str,
        schema_version: str,
        logical_date: date,
        source_file_name: str,
        landing_uri: str,
        content_type: str,
        extracted_at: datetime,
        landed_at: datetime,
        record_count: int,
        file_size_bytes: int,
        sha256: str,
        batch_id: UUID | None = None,
    ) -> BatchManifest:
        """Create a validated manifest and derive its idempotency key."""

        idempotency_key = build_batch_idempotency_key(
            source_system=source_system,
            dataset=dataset,
            logical_date=logical_date,
            schema_version=schema_version,
            artifact_sha256=sha256,
        )

        return cls(
            manifest_version=cls.CURRENT_VERSION,
            batch_id=batch_id or uuid4(),
            idempotency_key=idempotency_key,
            source_system=source_system,
            dataset=dataset,
            schema_version=schema_version,
            logical_date=logical_date,
            source_file_name=source_file_name,
            landing_uri=landing_uri,
            content_type=content_type,
            extracted_at=extracted_at,
            landed_at=landed_at,
            record_count=record_count,
            file_size_bytes=file_size_bytes,
            sha256=sha256,
        )
