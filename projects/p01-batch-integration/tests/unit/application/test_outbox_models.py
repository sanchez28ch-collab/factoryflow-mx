"""Tests for validated transactional-outbox application models."""

from datetime import timedelta
from unittest import TestCase

from factoryflow_batch.application.exceptions import (
    OutboxPublicationConfigurationError,
    OutboxPublicationConsistencyError,
)
from factoryflow_batch.application.outbox_models import (
    OutboxPublicationPolicy,
    OutboxPublicationReport,
)


class TestOutboxPublicationPolicy(TestCase):
    """Verify safe operational limits for publisher executions."""

    def test_uses_production_safe_defaults(self) -> None:
        policy = OutboxPublicationPolicy()

        self.assertEqual(policy.batch_size, 100)
        self.assertEqual(policy.lease_duration, timedelta(seconds=30))
        self.assertEqual(policy.max_attempts, 10)
        self.assertEqual(policy.retry_base_delay, timedelta(seconds=1))
        self.assertEqual(policy.retry_max_delay, timedelta(minutes=5))

    def test_accepts_boundary_values(self) -> None:
        policy = OutboxPublicationPolicy(
            batch_size=1_000,
            lease_duration=timedelta(minutes=15),
            max_attempts=100,
            retry_base_delay=timedelta(hours=1),
            retry_max_delay=timedelta(days=1),
        )

        self.assertEqual(policy.batch_size, 1_000)
        self.assertEqual(policy.max_attempts, 100)

    def test_rejects_invalid_integer_settings(self) -> None:
        cases = (
            ("batch_size", 0),
            ("batch_size", 1_001),
            ("batch_size", True),
            ("max_attempts", 0),
            ("max_attempts", 101),
            ("max_attempts", False),
        )

        for field_name, value in cases:
            with (
                self.subTest(field_name=field_name, value=value),
                self.assertRaisesRegex(
                    OutboxPublicationConfigurationError,
                    field_name,
                ),
            ):
                OutboxPublicationPolicy(**{field_name: value})

    def test_rejects_invalid_durations(self) -> None:
        cases = (
            ("lease_duration", timedelta(0)),
            ("lease_duration", timedelta(minutes=16)),
            ("retry_base_delay", timedelta(seconds=-1)),
            ("retry_base_delay", timedelta(hours=2)),
            ("retry_max_delay", timedelta(0)),
            ("retry_max_delay", timedelta(days=2)),
        )

        for field_name, value in cases:
            with (
                self.subTest(field_name=field_name, value=value),
                self.assertRaisesRegex(
                    OutboxPublicationConfigurationError,
                    field_name,
                ),
            ):
                OutboxPublicationPolicy(**{field_name: value})

    def test_rejects_retry_maximum_shorter_than_base(self) -> None:
        with self.assertRaisesRegex(
            OutboxPublicationConfigurationError,
            "retry_max_delay",
        ):
            OutboxPublicationPolicy(
                retry_base_delay=timedelta(minutes=2),
                retry_max_delay=timedelta(minutes=1),
            )


class TestOutboxPublicationReport(TestCase):
    """Verify complete and nonnegative outcome accounting."""

    def test_reports_idle_and_changed_executions(self) -> None:
        idle = OutboxPublicationReport(0, 0, 0, 0)
        changed = OutboxPublicationReport(3, 1, 1, 1)

        self.assertFalse(idle.changed)
        self.assertTrue(changed.changed)

    def test_rejects_invalid_counters(self) -> None:
        cases = (
            (-1, 0, 0, -1),
            (1, True, 0, 0),
            (1, 0, -1, 2),
        )

        for claimed, published, rescheduled, dead_lettered in cases:
            with (
                self.subTest(claimed=claimed),
                self.assertRaisesRegex(
                    OutboxPublicationConsistencyError,
                    "nonnegative integers",
                ),
            ):
                OutboxPublicationReport(
                    claimed,
                    published,
                    rescheduled,
                    dead_lettered,
                )

    def test_rejects_missing_or_duplicate_outcomes(self) -> None:
        for outcomes in ((2, 1, 0, 0), (1, 1, 1, 0)):
            with (
                self.subTest(outcomes=outcomes),
                self.assertRaisesRegex(
                    OutboxPublicationConsistencyError,
                    "do not match",
                ),
            ):
                OutboxPublicationReport(*outcomes)
