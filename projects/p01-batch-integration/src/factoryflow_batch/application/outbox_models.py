"""Validated models used by the transactional outbox publisher."""

from dataclasses import dataclass
from datetime import timedelta

from factoryflow_batch.application.exceptions import (
    OutboxPublicationConfigurationError,
    OutboxPublicationConsistencyError,
)


@dataclass(frozen=True, slots=True)
class OutboxPublicationPolicy:
    """Operational limits governing one publisher execution."""

    batch_size: int = 100
    lease_duration: timedelta = timedelta(seconds=30)
    max_attempts: int = 10
    retry_base_delay: timedelta = timedelta(seconds=1)
    retry_max_delay: timedelta = timedelta(minutes=5)

    def __post_init__(self) -> None:
        """Reject unsafe or operationally unreasonable settings."""

        _validate_integer(
            name="batch_size",
            value=self.batch_size,
            minimum=1,
            maximum=1_000,
        )
        _validate_integer(
            name="max_attempts",
            value=self.max_attempts,
            minimum=1,
            maximum=100,
        )
        _validate_duration(
            name="lease_duration",
            value=self.lease_duration,
            maximum=timedelta(minutes=15),
        )
        _validate_duration(
            name="retry_base_delay",
            value=self.retry_base_delay,
            maximum=timedelta(hours=1),
        )
        _validate_duration(
            name="retry_max_delay",
            value=self.retry_max_delay,
            maximum=timedelta(days=1),
        )

        if self.retry_max_delay < self.retry_base_delay:
            raise OutboxPublicationConfigurationError(
                "retry_max_delay must not be shorter than retry_base_delay."
            )


@dataclass(frozen=True, slots=True)
class OutboxPublicationReport:
    """Aggregate publication outcomes without event payload disclosure."""

    claimed: int
    published: int
    rescheduled: int
    dead_lettered: int

    def __post_init__(self) -> None:
        """Ensure every claimed event has exactly one recorded outcome."""

        values = (
            self.claimed,
            self.published,
            self.rescheduled,
            self.dead_lettered,
        )
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in values
        ):
            raise OutboxPublicationConsistencyError(
                "Outbox report counters must be nonnegative integers."
            )

        outcomes = self.published + self.rescheduled + self.dead_lettered
        if outcomes != self.claimed:
            raise OutboxPublicationConsistencyError(
                "Outbox report outcomes do not match claimed events."
            )

    @property
    def changed(self) -> bool:
        """Return whether the execution processed at least one event."""

        return self.claimed > 0


def _validate_integer(
    *,
    name: str,
    value: int,
    minimum: int,
    maximum: int,
) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise OutboxPublicationConfigurationError(
            f"{name} must be an integer between {minimum} and {maximum}."
        )


def _validate_duration(
    *,
    name: str,
    value: timedelta,
    maximum: timedelta,
) -> None:
    if not isinstance(value, timedelta) or not timedelta(0) < value <= maximum:
        raise OutboxPublicationConfigurationError(
            f"{name} must be positive and no greater than {maximum}."
        )
