"""Tests for deterministic transactional-outbox retry delays."""

from datetime import timedelta
from unittest import TestCase
from uuid import UUID

from factoryflow_batch.application.exceptions import (
    OutboxPublicationConsistencyError,
)
from factoryflow_batch.application.outbox_backoff import (
    calculate_retry_delay,
)
from factoryflow_batch.application.outbox_models import (
    OutboxPublicationPolicy,
)

_EVENT_ID = UUID("0b256e5e-25a9-4c14-a8c2-1c9f2fffb100")


class TestOutboxRetryBackoff(TestCase):
    """Verify deterministic, bounded and distributed retry delays."""

    def setUp(self) -> None:
        self.policy = OutboxPublicationPolicy(
            max_attempts=10,
            retry_base_delay=timedelta(seconds=2),
            retry_max_delay=timedelta(seconds=20),
        )

    def test_is_deterministic_for_same_event_and_attempt(self) -> None:
        first = calculate_retry_delay(
            policy=self.policy,
            event_id=_EVENT_ID,
            attempt_count=3,
        )
        second = calculate_retry_delay(
            policy=self.policy,
            event_id=_EVENT_ID,
            attempt_count=3,
        )

        self.assertEqual(first, second)

    def test_stays_within_equal_jitter_window(self) -> None:
        for attempt_count in range(1, 11):
            with self.subTest(attempt_count=attempt_count):
                delay = calculate_retry_delay(
                    policy=self.policy,
                    event_id=_EVENT_ID,
                    attempt_count=attempt_count,
                )
                cap = min(
                    2 * (2 ** (attempt_count - 1)),
                    20,
                )

                self.assertGreaterEqual(
                    delay,
                    timedelta(seconds=cap / 2),
                )
                self.assertLessEqual(
                    delay,
                    timedelta(seconds=cap),
                )

    def test_changes_between_attempts(self) -> None:
        delays = {
            calculate_retry_delay(
                policy=self.policy,
                event_id=_EVENT_ID,
                attempt_count=attempt,
            )
            for attempt in range(1, 5)
        }

        self.assertEqual(len(delays), 4)

    def test_rejects_invalid_attempt_counts(self) -> None:
        for attempt_count in (0, 11, True):
            with (
                self.subTest(attempt_count=attempt_count),
                self.assertRaisesRegex(
                    OutboxPublicationConsistencyError,
                    "attempt_count",
                ),
            ):
                calculate_retry_delay(
                    policy=self.policy,
                    event_id=_EVENT_ID,
                    attempt_count=attempt_count,
                )

    def test_rejects_zero_event_identifier(self) -> None:
        with self.assertRaisesRegex(
            OutboxPublicationConsistencyError,
            "event_id",
        ):
            calculate_retry_delay(
                policy=self.policy,
                event_id=UUID(int=0),
                attempt_count=1,
            )
