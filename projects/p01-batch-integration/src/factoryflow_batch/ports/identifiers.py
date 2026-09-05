"""Identifier-generation boundary used by application services."""

from typing import Protocol
from uuid import UUID


class BatchIdGenerator(Protocol):
    """Generate globally unique identifiers for accepted batches."""

    def new_batch_id(self) -> UUID:
        """Return a new non-zero batch identifier."""
