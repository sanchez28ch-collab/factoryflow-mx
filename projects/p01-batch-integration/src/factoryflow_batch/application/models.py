"""Application input and output models for batch ingestion."""

from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum

from factoryflow_batch.domain import BatchManifest


@dataclass(frozen=True, slots=True)
class IngestBatchCommand:
    """Request to ingest one immutable source artifact."""

    source_system: str
    dataset: str
    schema_version: str
    logical_date: date
    source_uri: str
    extracted_at: datetime


class BatchIngestionDisposition(StrEnum):
    """Observable result of an idempotent ingestion request."""

    CREATED = "created"
    DEDUPLICATED = "deduplicated"


@dataclass(frozen=True, slots=True)
class BatchIngestionResult:
    """Manifest and disposition returned to an ingestion caller."""

    disposition: BatchIngestionDisposition
    manifest: BatchManifest
