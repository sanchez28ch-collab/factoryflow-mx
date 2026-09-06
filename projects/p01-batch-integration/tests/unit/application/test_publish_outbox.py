"""Behavior tests for transactional-outbox publication."""

from datetime import UTC, datetime, timedelta
from unittest import TestCase
from uuid import UUID

from factoryflow_batch.application.outbox_models import (
    OutboxPublicationPolicy,
    OutboxPublicationReport,
)
from factoryflow_batch.application.publish_outbox import (
    OutboxPublisherService,
)
from factoryflow_batch.ports import (
    EventPublicationError,
    LeasedOutboxEvent,
)
from tests.fakes.outbox import (
    FakeEventPublisher,
    FakeOutboxStore,
    FixedClock,
    FixedLeaseIdGenerator,
)

_NOW = datetime(2026, 9, 5, 6, 0, tzinfo=UTC)
_LEASE_ID = "worker-01:lease-001"
_EVENT_ID = UUID("c4bdfb0d-83b2-41fb-9be2-a01d64f96e10")


def _event(
    *,
    event_id: UUID = _EVENT_ID,
    attempt_count: int = 1,
    lease_id: str = _LEASE_ID,
) -> LeasedOutboxEvent:
    return LeasedOutboxEvent(
        event_id=event_id,
        destination_topic="factoryflow.batch.ingested.v1",
        partition_key="batch-001",
        payload=b'{"event_type":"factoryflow.batch.ingested"}',
        headers=(("content-type", b"application/json"),),
        occurred_at=_NOW,
        attempt_count=attempt_count,
        lease_id=lease_id,
    )


def _service(
    store: FakeOutboxStore,
    publisher: FakeEventPublisher,
    *,
    policy: OutboxPublicationPolicy | None = None,
) -> OutboxPublisherService:
    return OutboxPublisherService(
        store=store,
        publisher=publisher,
        clock=FixedClock(_NOW),
        lease_ids=FixedLeaseIdGenerator(_LEASE_ID),
        policy=policy,
    )


class TestOutboxPublisherService(TestCase):
    """Verify successful, retryable and terminal publication paths."""

    def test_idle_execution_claims_one_bounded_batch(self) -> None:
        store = FakeOutboxStore()
        publisher = FakeEventPublisher()

        report = _service(store, publisher).run_once()

        self.assertEqual(report, OutboxPublicationReport(0, 0, 0, 0))
        self.assertEqual(
            store.claims,
            [
                (
                    _LEASE_ID,
                    _NOW,
                    _NOW + timedelta(seconds=30),
                    100,
                )
            ],
        )
        self.assertEqual(publisher.events, [])

    def test_publishes_and_acknowledges_every_claimed_event(self) -> None:
        second_id = UUID("c4bdfb0d-83b2-41fb-9be2-a01d64f96e11")
        events = (_event(), _event(event_id=second_id))
        store = FakeOutboxStore(events)
        publisher = FakeEventPublisher()

        report = _service(store, publisher).run_once()

        self.assertEqual(report, OutboxPublicationReport(2, 2, 0, 0))
        self.assertEqual(publisher.events, list(events))
        self.assertEqual(
            [record[0] for record in store.published],
            [_EVENT_ID, second_id],
        )
        self.assertEqual(store.rescheduled, [])
        self.assertEqual(store.dead_lettered, [])

    def test_reschedules_retryable_failure_with_bounded_backoff(self) -> None:
        event = _event(attempt_count=2)
        store = FakeOutboxStore((event,))
        publisher = FakeEventPublisher()
        publisher.failures[_EVENT_ID] = EventPublicationError(retryable=True)

        report = _service(store, publisher).run_once()

        self.assertEqual(report, OutboxPublicationReport(1, 0, 1, 0))
        _, lease_id, available_at, error_code = store.rescheduled[0]
        self.assertEqual(lease_id, _LEASE_ID)
        self.assertEqual(error_code, "publisher_retryable")
        self.assertGreaterEqual(available_at, _NOW + timedelta(seconds=1))
        self.assertLessEqual(available_at, _NOW + timedelta(seconds=2))

    def test_dead_letters_nonretryable_failure(self) -> None:
        store = FakeOutboxStore((_event(),))
        publisher = FakeEventPublisher()
        publisher.failures[_EVENT_ID] = EventPublicationError(retryable=False)

        report = _service(store, publisher).run_once()

        self.assertEqual(report, OutboxPublicationReport(1, 0, 0, 1))
        self.assertEqual(
            store.dead_lettered[0],
            (
                _EVENT_ID,
                _LEASE_ID,
                _NOW,
                "publisher_non_retryable",
            ),
        )

    def test_dead_letters_retryable_failure_at_attempt_limit(self) -> None:
        policy = OutboxPublicationPolicy(max_attempts=3)
        store = FakeOutboxStore((_event(attempt_count=3),))
        publisher = FakeEventPublisher()
        publisher.failures[_EVENT_ID] = EventPublicationError(retryable=True)

        report = _service(store, publisher, policy=policy).run_once()

        self.assertEqual(report.dead_lettered, 1)
        self.assertEqual(
            store.dead_lettered[0][3],
            "attempt_limit_reached",
        )

    def test_dead_letters_previously_exhausted_event_without_publishing(
        self,
    ) -> None:
        policy = OutboxPublicationPolicy(max_attempts=3)
        store = FakeOutboxStore((_event(attempt_count=4),))
        publisher = FakeEventPublisher()

        report = _service(store, publisher, policy=policy).run_once()

        self.assertEqual(report, OutboxPublicationReport(1, 0, 0, 1))
        self.assertEqual(publisher.events, [])
        self.assertEqual(
            store.dead_lettered[0][3],
            "attempt_limit_exceeded",
        )
