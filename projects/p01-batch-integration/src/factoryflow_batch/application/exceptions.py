"""Application-level failures for the P01 ingestion workflow."""


class BatchIngestionError(Exception):
    """Base class for controlled batch-ingestion failures."""


class BatchIngestionValidationError(BatchIngestionError):
    """Raised before external side effects when a command is invalid."""


class ArtifactIntegrityError(BatchIngestionError):
    """Raised when landed bytes differ from the inspected source."""


class BatchRegistryConsistencyError(BatchIngestionError):
    """Raised when persistence returns an incompatible registration."""


class OutboxPublicationError(BatchIngestionError):
    """Base class for controlled outbox-publication failures."""


class OutboxPublicationConfigurationError(OutboxPublicationError):
    """Raised when a publisher policy or runtime dependency is invalid."""


class OutboxPublicationConsistencyError(OutboxPublicationError):
    """Raised when a publication outcome cannot be persisted safely."""


class OutboxPublicationPersistenceError(OutboxPublicationError):
    """Raised when publication state cannot be persisted safely."""
