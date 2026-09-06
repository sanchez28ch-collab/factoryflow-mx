"""Application service coordinating transactional-outbox publication."""

import re
from datetime import UTC, datetime
from enum import Enum

from factoryflow_batch.application.exceptions import (
    OutboxPublicationConfigurationError,
    OutboxPublicationConsistencyError,
    OutboxPublicationPersistenceError,
)
from factoryflow_batch.application.outbox_backoff import (
    calculate_retry_delay,
)
from factoryflow_batch.application.outbox_models import (
    OutboxPublicationPolicy,
    OutboxPublicationReport,
)
from factoryflow_batch.ports import (
    Clock,
    EventPublicationError,
    EventPublisher,
    LeasedOutboxEvent,
    LeaseIdGenerator,
    OutboxStore,
    OutboxStoreError,
    PublicationReceipt,
)

_LEASE_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


class _Outcome(Enum):
    PUBLISHED = "published"
    RESCHEDULED = "rescheduled"
    DEAD_LETTERED = "dead_lettered"


class OutboxPublisherService:
    """Publish one bounded leased batch with at-least-once delivery."""

    def __init__(
        self,
        *,
        store: OutboxStore,
        publisher: EventPublisher,
        clock: Clock,
        lease_ids: LeaseIdGenerator,
        policy: OutboxPublicationPolicy | None = None,
    ) -> None:
        self._store = store
        self._publisher = publisher
        self._clock = clock
        self._lease_ids = lease_ids
        self._policy = policy or OutboxPublicationPolicy()

    def run_once(self) -> OutboxPublicationReport:
        """Process one available batch and return non-sensitive counters."""

        events = self._claim_pending()
        outcomes = tuple(self._process_event(event) for event in events)

        return OutboxPublicationReport(
            claimed=len(events),
            published=outcomes.count(_Outcome.PUBLISHED),
            rescheduled=outcomes.count(_Outcome.RESCHEDULED),
            dead_lettered=outcomes.count(_Outcome.DEAD_LETTERED),
        )

    def _claim_pending(self) -> tuple[LeasedOutboxEvent, ...]:
        now = _utc_now(self._clock)
        lease_id = self._lease_ids.new_lease_id()
        _validate_lease_id(lease_id)

        try:
            events = self._store.claim_pending(
                lease_id=lease_id,
                now=now,
                lease_expires_at=now + self._policy.lease_duration,
                limit=self._policy.batch_size,
            )
        except OutboxStoreError as exc:
            raise OutboxPublicationPersistenceError(
                "Pending outbox events could not be claimed."
            ) from exc

        for event in events:
            _validate_claimed_event(event=event, lease_id=lease_id)

        return events

    def _process_event(self, event: LeasedOutboxEvent) -> _Outcome:
        if event.attempt_count > self._policy.max_attempts:
            self._mark_dead_lettered(
                event=event,
                error_code="attempt_limit_exceeded",
            )
            return _Outcome.DEAD_LETTERED

        try:
            receipt = self._publisher.publish(event)
        except EventPublicationError as exc:
            return self._record_publication_failure(event=event, failure=exc)

        _validate_receipt(event=event, receipt=receipt)
        self._mark_published(event)
        return _Outcome.PUBLISHED

    def _record_publication_failure(
        self,
        *,
        event: LeasedOutboxEvent,
        failure: EventPublicationError,
    ) -> _Outcome:
        if not failure.retryable or event.attempt_count >= self._policy.max_attempts:
            error_code = (
                "publisher_non_retryable" if not failure.retryable else "attempt_limit_reached"
            )
            self._mark_dead_lettered(
                event=event,
                error_code=error_code,
            )
            return _Outcome.DEAD_LETTERED

        failed_at = _utc_now(self._clock)
        retry_delay = calculate_retry_delay(
            policy=self._policy,
            event_id=event.event_id,
            attempt_count=event.attempt_count,
        )

        try:
            self._store.reschedule(
                event_id=event.event_id,
                lease_id=event.lease_id,
                available_at=failed_at + retry_delay,
                error_code="publisher_retryable",
            )
        except OutboxStoreError as exc:
            raise OutboxPublicationPersistenceError(
                "The retry outcome could not be persisted."
            ) from exc

        return _Outcome.RESCHEDULED

    def _mark_published(self, event: LeasedOutboxEvent) -> None:
        try:
            self._store.mark_published(
                event_id=event.event_id,
                lease_id=event.lease_id,
                published_at=_utc_now(self._clock),
            )
        except OutboxStoreError as exc:
            raise OutboxPublicationConsistencyError(
                "Broker acknowledgement could not be committed."
            ) from exc

    def _mark_dead_lettered(
        self,
        *,
        event: LeasedOutboxEvent,
        error_code: str,
    ) -> None:
        try:
            self._store.mark_dead_lettered(
                event_id=event.event_id,
                lease_id=event.lease_id,
                dead_lettered_at=_utc_now(self._clock),
                error_code=error_code,
            )
        except OutboxStoreError as exc:
            raise OutboxPublicationPersistenceError(
                "The terminal outbox outcome could not be persisted."
            ) from exc


def _utc_now(clock: Clock) -> datetime:
    value = clock.now()

    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise OutboxPublicationConfigurationError("Clock values must be timezone-aware datetimes.")

    return value.astimezone(UTC)


def _validate_lease_id(lease_id: str) -> None:
    if not isinstance(lease_id, str) or not _LEASE_ID_PATTERN.fullmatch(lease_id):
        raise OutboxPublicationConfigurationError(
            "Generated lease identifiers must use the supported format."
        )


def _validate_claimed_event(
    *,
    event: LeasedOutboxEvent,
    lease_id: str,
) -> None:
    if event.lease_id != lease_id:
        raise OutboxPublicationConsistencyError(
            "A claimed event contains an unexpected lease identifier."
        )

    if (
        isinstance(event.attempt_count, bool)
        or not isinstance(event.attempt_count, int)
        or event.attempt_count < 1
    ):
        raise OutboxPublicationConsistencyError(
            "A claimed event contains an invalid attempt count."
        )


def _validate_receipt(
    *,
    event: LeasedOutboxEvent,
    receipt: PublicationReceipt,
) -> None:
    if (
        receipt.topic != event.destination_topic
        or isinstance(receipt.partition, bool)
        or not isinstance(receipt.partition, int)
        or receipt.partition < 0
        or isinstance(receipt.offset, bool)
        or not isinstance(receipt.offset, int)
        or receipt.offset < 0
    ):
        raise OutboxPublicationConsistencyError(
            "The publisher returned an invalid acknowledgement."
        )
