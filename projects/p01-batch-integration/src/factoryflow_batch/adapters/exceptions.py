"""Controlled failures raised by concrete P01 infrastructure adapters."""


class LocalAdapterError(Exception):
    """Base failure for local infrastructure adapters."""


class LocalArtifactAccessError(LocalAdapterError):
    """Raised when a local source cannot be accessed safely."""


class LocalArtifactIntegrityError(LocalAdapterError):
    """Raised when source bytes differ from their expected digest."""


class CsvArtifactValidationError(LocalAdapterError):
    """Raised when a CSV artifact is malformed or structurally invalid."""


class ImmutableLandingConflictError(LocalAdapterError):
    """Raised when an immutable destination contains different bytes."""
