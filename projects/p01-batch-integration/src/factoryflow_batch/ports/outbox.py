"""Ports for leasing and publishing transactional outbox events."""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID


@dataclass(frozen=True, slots=True)
class LeasedOutboxEvent:
    """Immutable event claimed exclusively for one publication attempt."""

    event_id: UUID
    destination_topic: str
    partition_key: str
    payload: bytes
    headers: tuple[tuple[str, bytes], ...]
    occurred_at: datetime
    attempt_count: int
    lease_id: str


@dataclass(frozen=True, slots=True)
class PublicationReceipt:
    """Kafka coordinates returned after broker acknowledgement."""

    topic: str
    partition: int
    offset: int


class OutboxStore(Protocol):
    """Lease events and persist publication outcomes atomically."""

    def claim_pending(
        self,
        *,
        lease_id: str,
        now: datetime,
        lease_expires_at: datetime,
        limit: int,
    ) -> tuple[LeasedOutboxEvent, ...]:
        """Claim available events using one unique opaque lease identifier."""

    def mark_published(
        self,
        *,
        event_id: UUID,
        lease_id: str,
        published_at: datetime,
    ) -> None:
        """Mark a leased event as published if the lease is still owned."""

    def reschedule(
        self,
        *,
        event_id: UUID,
        lease_id: str,
        available_at: datetime,
        error_code: str,
    ) -> None:
        """Release a failed event for a later controlled retry."""

    def mark_dead_lettered(
        self,
        *,
        event_id: UUID,
        lease_id: str,
        dead_lettered_at: datetime,
        error_code: str,
    ) -> None:
        """Move an exhausted event into its terminal failure state."""


class EventPublisher(Protocol):
    """Publish one encoded event and wait for broker acknowledgement."""

    def publish(self, event: LeasedOutboxEvent, /) -> PublicationReceipt:
        """Return broker coordinates only after confirmed publication."""


class EventPublicationError(Exception):
    """Controlled publisher failure without infrastructure-detail leakage."""

    def __init__(self, *, retryable: bool) -> None:
        super().__init__("Event publication failed.")
        self.retryable = retryable


class OutboxStoreError(Exception):
    """Controlled failure while accessing transactional outbox state."""


class OutboxLeaseLostError(OutboxStoreError):
    """Raised when an outcome cannot be committed under the expected lease."""


class Clock(Protocol):
    """Provide timezone-aware application time."""

    def now(self) -> datetime:
        """Return the current timezone-aware instant."""


class LeaseIdGenerator(Protocol):
    """Generate a unique opaque identifier for each claim operation."""

    def new_lease_id(self) -> str:
        """Return a unique lease identifier."""
