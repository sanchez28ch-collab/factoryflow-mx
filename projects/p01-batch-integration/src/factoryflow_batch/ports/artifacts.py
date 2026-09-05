"""Ports for inspecting and landing immutable batch artifacts."""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True, slots=True)
class InspectedArtifact:
    """Metadata calculated by streaming a source artifact once."""

    source_file_name: str
    content_type: str
    record_count: int
    file_size_bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class LandingReceipt:
    """Authoritative metadata returned by immutable landing storage."""

    landing_uri: str
    landed_at: datetime
    file_size_bytes: int
    sha256: str


class ArtifactInspector(Protocol):
    """Inspect an artifact without loading the complete file into memory."""

    def inspect(self, source_uri: str, /) -> InspectedArtifact:
        """Return content metadata and integrity information."""


class LandingStore(Protocol):
    """Persist source artifacts under deterministic object keys."""

    def land(
        self,
        *,
        source_uri: str,
        object_key: str,
        expected_sha256: str,
    ) -> LandingReceipt:
        """Land an artifact idempotently and return its storage receipt."""
