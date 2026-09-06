"""Unit tests for concrete outbox runtime adapters."""

import re
from datetime import UTC, datetime
from unittest import TestCase
from uuid import UUID

from factoryflow_batch.adapters.runtime import (
    SystemUtcClock,
    UuidLeaseIdGenerator,
)

_LEASE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


class TestSystemUtcClock(TestCase):
    """Verify timezone-aware system time."""

    def test_returns_current_utc_timestamp(self) -> None:
        before = datetime.now(UTC)
        value = SystemUtcClock().now()
        after = datetime.now(UTC)

        self.assertIs(value.tzinfo, UTC)
        self.assertLessEqual(before, value)
        self.assertLessEqual(value, after)


class TestUuidLeaseIdGenerator(TestCase):
    """Verify safe globally unique lease ownership tokens."""

    def test_generates_unique_valid_identifiers(self) -> None:
        generator = UuidLeaseIdGenerator(worker_id="factoryflow-worker-01")

        first = generator.new_lease_id()
        second = generator.new_lease_id()

        self.assertNotEqual(first, second)
        self.assertLessEqual(len(first), 128)
        self.assertIsNotNone(_LEASE_PATTERN.fullmatch(first))

        prefix, raw_uuid = first.split(":", maxsplit=1)
        self.assertEqual(prefix, "factoryflow-worker-01")
        self.assertNotEqual(UUID(raw_uuid).int, 0)

    def test_accepts_maximum_worker_identifier_length(
        self,
    ) -> None:
        worker_id = "w" * 90
        lease_id = UuidLeaseIdGenerator(worker_id=worker_id).new_lease_id()

        self.assertEqual(len(lease_id), 127)

    def test_rejects_unsafe_worker_identifiers(self) -> None:
        invalid_values = (
            "",
            "-worker",
            "worker with spaces",
            "worker:reserved",
            "worker/path",
            "x" * 91,
        )

        for worker_id in invalid_values:
            with (
                self.subTest(worker_id=worker_id),
                self.assertRaisesRegex(
                    ValueError,
                    "worker_id",
                ),
            ):
                UuidLeaseIdGenerator(worker_id=worker_id)
