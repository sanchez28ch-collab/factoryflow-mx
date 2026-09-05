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


class PostgresBatchRegistryError(LocalAdapterError):
    """Raised when the PostgreSQL batch registry cannot complete an operation."""


class PostgresBatchRegistryDataError(PostgresBatchRegistryError):
    """Raised when persisted registry data violates the domain contract."""


class PostgresMigrationError(LocalAdapterError):
    """Base failure for controlled PostgreSQL migration operations."""


class PostgresMigrationDiscoveryError(PostgresMigrationError):
    """Raised when migration artifacts cannot be discovered safely."""


class PostgresMigrationDriftError(PostgresMigrationError):
    """Raised when applied migrations differ from repository artifacts."""


class PostgresMigrationExecutionError(PostgresMigrationError):
    """Raised when PostgreSQL cannot apply a pending migration."""
