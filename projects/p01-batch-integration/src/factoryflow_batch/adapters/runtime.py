"""Runtime adapters for clocks and unique outbox lease identifiers."""

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

_WORKER_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,89}$")


class SystemUtcClock:
    """Return timezone-aware UTC values from the system clock."""

    def now(self) -> datetime:
        """Return the current UTC timestamp."""

        return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class UuidLeaseIdGenerator:
    """Generate globally unique lease identifiers for one worker."""

    worker_id: str = "factoryflow-p01-outbox"

    def __post_init__(self) -> None:
        """Reject worker identifiers unsafe for persistence or logs."""

        if (
            not isinstance(self.worker_id, str)
            or _WORKER_ID_PATTERN.fullmatch(self.worker_id) is None
        ):
            raise ValueError("worker_id uses an unsupported format.")

    def new_lease_id(self) -> str:
        """Return a unique lease identifier within the 128-byte limit."""

        return f"{self.worker_id}:{uuid4()}"
