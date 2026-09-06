"""Adversarial tests for transactional-outbox publication."""

from datetime import UTC, datetime
from unittest import TestCase
from uuid import UUID

from factoryflow_batch.application.exceptions import (
    OutboxPublicationConfigurationError,
    OutboxPublicationConsistencyError,
    OutboxPublicationPersistenceError,
)
from factoryflow_batch.application.publish_outbox import (
    OutboxPublisherService,
)
from factoryflow_batch.ports import (
    EventPublicationError,
    LeasedOutboxEvent,
    PublicationReceipt,
)
from tests.fakes.outbox import (
    FakeEventPublisher,
    FakeOutboxStore,
    FixedClock,
    FixedLeaseIdGenerator,
)

_NOW = datetime(2026, 9, 5, 6, 30, tzinfo=UTC)
_LEASE_ID = "worker-01:lease-adversarial"
_EVENT_ID = UUID("4e932bc4-e45f-4faa-8f48-065b6d4be630")


def _event(
    *,
    lease_id: str = _LEASE_ID,
    attempt_count: int = 1,
) -> LeasedOutboxEvent:
    return LeasedOutboxEvent(
        event_id=_EVENT_ID,
        destination_topic="factoryflow.batch.ingested.v1",
        partition_key="batch-001",
        payload=b'{"event_id":"4e932bc4-e45f-4faa-8f48-065b6d4be630"}',
        headers=(("content-type", b"application/json"),),
        occurred_at=_NOW,
        attempt_count=attempt_count,
        lease_id=lease_id,
    )


def _service(
    store: FakeOutboxStore,
    publisher: FakeEventPublisher,
    *,
    clock: datetime = _NOW,
    lease_id: str = _LEASE_ID,
) -> OutboxPublisherService:
    return OutboxPublisherService(
        store=store,
        publisher=publisher,
        clock=FixedClock(clock),
        lease_ids=FixedLeaseIdGenerator(lease_id),
    )


class TestOutboxPublisherFailures(TestCase):
    """Verify fail-closed behavior and controlled error disclosure."""

    def test_translates_claim_failure_without_leaking_details(self) -> None:
        store = FakeOutboxStore()
        store.fail_claim = True
        publisher = FakeEventPublisher()

        with self.assertRaisesRegex(
            OutboxPublicationPersistenceError,
            "could not be claimed",
        ) as raised:
            _service(store, publisher).run_once()

        self.assertNotIn("injected", str(raised.exception))
        self.assertEqual(publisher.events, [])

    def test_acknowledgement_persistence_failure_is_consistency_error(
        self,
    ) -> None:
        store = FakeOutboxStore((_event(),))
        store.fail_mark_published = True

        with self.assertRaisesRegex(
            OutboxPublicationConsistencyError,
            "acknowledgement",
        ):
            _service(store, FakeEventPublisher()).run_once()

    def test_retry_persistence_failure_is_controlled(self) -> None:
        store = FakeOutboxStore((_event(),))
        store.fail_reschedule = True
        publisher = FakeEventPublisher()
        publisher.failures[_EVENT_ID] = EventPublicationError(retryable=True)

        with self.assertRaisesRegex(
            OutboxPublicationPersistenceError,
            "retry outcome",
        ):
            _service(store, publisher).run_once()

    def test_dead_letter_persistence_failure_is_controlled(self) -> None:
        store = FakeOutboxStore((_event(),))
        store.fail_dead_letter = True
        publisher = FakeEventPublisher()
        publisher.failures[_EVENT_ID] = EventPublicationError(retryable=False)

        with self.assertRaisesRegex(
            OutboxPublicationPersistenceError,
            "terminal outbox outcome",
        ):
            _service(store, publisher).run_once()

    def test_rejects_naive_clock_before_database_access(self) -> None:
        store = FakeOutboxStore()

        with self.assertRaisesRegex(
            OutboxPublicationConfigurationError,
            "timezone-aware",
        ):
            _service(
                store,
                FakeEventPublisher(),
                clock=datetime(2026, 9, 5, 6, 30),
            ).run_once()

        self.assertEqual(store.claims, [])

    def test_rejects_unsafe_lease_identifier_before_claim(self) -> None:
        store = FakeOutboxStore()

        with self.assertRaisesRegex(
            OutboxPublicationConfigurationError,
            "lease identifiers",
        ):
            _service(
                store,
                FakeEventPublisher(),
                lease_id="unsafe lease with spaces",
            ).run_once()

        self.assertEqual(store.claims, [])

    def test_rejects_event_owned_by_another_lease(self) -> None:
        store = FakeOutboxStore((_event(lease_id="another-lease"),))

        with self.assertRaisesRegex(
            OutboxPublicationConsistencyError,
            "unexpected lease",
        ):
            _service(store, FakeEventPublisher()).run_once()

    def test_rejects_nonpositive_attempt_count(self) -> None:
        store = FakeOutboxStore((_event(attempt_count=0),))

        with self.assertRaisesRegex(
            OutboxPublicationConsistencyError,
            "attempt count",
        ):
            _service(store, FakeEventPublisher()).run_once()

    def test_rejects_invalid_broker_acknowledgements(self) -> None:
        receipts = (
            PublicationReceipt("unexpected.topic", 0, 1),
            PublicationReceipt(
                "factoryflow.batch.ingested.v1",
                -1,
                1,
            ),
            PublicationReceipt(
                "factoryflow.batch.ingested.v1",
                0,
                -1,
            ),
        )

        for receipt in receipts:
            with self.subTest(receipt=receipt):
                store = FakeOutboxStore((_event(),))
                publisher = FakeEventPublisher()
                publisher.receipts[_EVENT_ID] = receipt

                with self.assertRaisesRegex(
                    OutboxPublicationConsistencyError,
                    "invalid acknowledgement",
                ):
                    _service(store, publisher).run_once()

                self.assertEqual(store.published, [])
