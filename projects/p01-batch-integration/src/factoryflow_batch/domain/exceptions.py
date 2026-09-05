"""Domain exceptions for batch ingestion."""


class BatchManifestValidationError(ValueError):
    """Raised when batch manifest data violates a domain invariant."""
