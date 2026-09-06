"""Deterministic capped backoff for transactional-outbox retries."""

import hashlib
from datetime import timedelta
from uuid import UUID

from factoryflow_batch.application.exceptions import (
    OutboxPublicationConsistencyError,
)
from factoryflow_batch.application.outbox_models import (
    OutboxPublicationPolicy,
)

_UINT64_MAX = (1 << 64) - 1


def calculate_retry_delay(
    *,
    policy: OutboxPublicationPolicy,
    event_id: UUID,
    attempt_count: int,
) -> timedelta:
    """Return capped exponential delay with deterministic equal jitter."""

    if (
        isinstance(attempt_count, bool)
        or not isinstance(attempt_count, int)
        or not 1 <= attempt_count <= policy.max_attempts
    ):
        raise OutboxPublicationConsistencyError(
            "attempt_count is outside the configured retry policy."
        )

    if not isinstance(event_id, UUID) or event_id.int == 0:
        raise OutboxPublicationConsistencyError("event_id must be a non-zero UUID.")

    exponential_seconds = policy.retry_base_delay.total_seconds() * (2 ** (attempt_count - 1))
    capped_seconds = min(
        exponential_seconds,
        policy.retry_max_delay.total_seconds(),
    )

    entropy = hashlib.sha256(
        event_id.bytes
        + attempt_count.to_bytes(
            length=2,
            byteorder="big",
            signed=False,
        )
    ).digest()
    ratio = int.from_bytes(entropy[:8], "big") / _UINT64_MAX
    jittered_seconds = capped_seconds * (0.5 + (ratio / 2))

    return timedelta(seconds=jittered_seconds)
