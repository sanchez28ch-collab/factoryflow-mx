"""Deterministic test doubles for transactional-outbox publication."""

from datetime import datetime
from uuid import UUID

from factoryflow_batch.ports import (
    EventPublicationError,
    LeasedOutboxEvent,
    OutboxStoreError,
    PublicationReceipt,
)


class FixedClock:
    """Return one fixed timezone-aware instant."""

    def __init__(self, value: datetime) -> None:
        self.value = value

    def now(self) -> datetime:
        """Return the configured instant."""

        return self.value


class FixedLeaseIdGenerator:
    """Return one deterministic lease identifier."""

    def __init__(self, lease_id: str) -> None:
        self.lease_id = lease_id

    def new_lease_id(self) -> str:
        """Return the configured lease identifier."""

        return self.lease_id


class FakeOutboxStore:
    """Record outbox state transitions and inject controlled failures."""

    def __init__(
        self,
        events: tuple[LeasedOutboxEvent, ...] = (),
    ) -> None:
        self.events = events
        self.fail_claim = False
        self.fail_mark_published = False
        self.fail_reschedule = False
        self.fail_dead_letter = False
        self.claims: list[tuple[str, datetime, datetime, int]] = []
        self.published: list[tuple[UUID, str, datetime]] = []
        self.rescheduled: list[tuple[UUID, str, datetime, str]] = []
        self.dead_lettered: list[tuple[UUID, str, datetime, str]] = []

    def claim_pending(
        self,
        *,
        lease_id: str,
        now: datetime,
        lease_expires_at: datetime,
        limit: int,
    ) -> tuple[LeasedOutboxEvent, ...]:
        """Record the claim and return configured events."""

        if self.fail_claim:
            raise OutboxStoreError("injected claim failure")

        self.claims.append((lease_id, now, lease_expires_at, limit))
        return self.events

    def mark_published(
        self,
        *,
        event_id: UUID,
        lease_id: str,
        published_at: datetime,
    ) -> None:
        """Record a successful publication."""

        if self.fail_mark_published:
            raise OutboxStoreError("injected acknowledgement failure")

        self.published.append((event_id, lease_id, published_at))

    def reschedule(
        self,
        *,
        event_id: UUID,
        lease_id: str,
        available_at: datetime,
        error_code: str,
    ) -> None:
        """Record a controlled retry."""

        if self.fail_reschedule:
            raise OutboxStoreError("injected reschedule failure")

        self.rescheduled.append((event_id, lease_id, available_at, error_code))

    def mark_dead_lettered(
        self,
        *,
        event_id: UUID,
        lease_id: str,
        dead_lettered_at: datetime,
        error_code: str,
    ) -> None:
        """Record terminal exhaustion."""

        if self.fail_dead_letter:
            raise OutboxStoreError("injected dead-letter failure")

        self.dead_lettered.append((event_id, lease_id, dead_lettered_at, error_code))


class FakeEventPublisher:
    """Record publications and return or raise configured outcomes."""

    def __init__(self) -> None:
        self.events: list[LeasedOutboxEvent] = []
        self.failures: dict[UUID, EventPublicationError] = {}
        self.receipts: dict[UUID, PublicationReceipt] = {}

    def publish(
        self,
        event: LeasedOutboxEvent,
        /,
    ) -> PublicationReceipt:
        """Return an acknowledgement unless a failure was configured."""

        self.events.append(event)

        failure = self.failures.get(event.event_id)
        if failure is not None:
            raise failure

        return self.receipts.get(
            event.event_id,
            PublicationReceipt(
                topic=event.destination_topic,
                partition=0,
                offset=len(self.events) - 1,
            ),
        )
