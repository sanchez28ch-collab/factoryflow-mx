"""Persistence boundary for idempotent batch registration."""

from dataclasses import dataclass
from typing import Protocol

from factoryflow_batch.domain import BatchManifest


@dataclass(frozen=True, slots=True)
class BatchRegistration:
    """Atomic registration decision returned by the persistence adapter."""

    created: bool
    manifest: BatchManifest


class BatchRegistry(Protocol):
    """Store batch manifests behind a unique idempotency constraint."""

    def find_by_idempotency_key(
        self,
        idempotency_key: str,
        /,
    ) -> BatchManifest | None:
        """Return the existing manifest when the request was processed."""

    def register_if_absent(
        self,
        manifest: BatchManifest,
        /,
    ) -> BatchRegistration:
        """Atomically insert or return the manifest that already exists."""
