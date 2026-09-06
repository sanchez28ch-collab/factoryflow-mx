"""Concrete infrastructure adapters exposed by P01."""

from factoryflow_batch.adapters.exceptions import (
    CsvArtifactValidationError,
    ImmutableLandingConflictError,
    KafkaPublisherConfigurationError,
    LocalAdapterError,
    LocalArtifactAccessError,
    LocalArtifactIntegrityError,
    PostgresBatchRegistryDataError,
    PostgresBatchRegistryError,
    PostgresMigrationDiscoveryError,
    PostgresMigrationDriftError,
    PostgresMigrationError,
    PostgresMigrationExecutionError,
    PostgresOutboxStoreDataError,
    PostgresOutboxStoreError,
)
from factoryflow_batch.adapters.kafka_event_publisher import (
    ConfluentKafkaEventPublisher,
    KafkaPublisherConfiguration,
)
from factoryflow_batch.adapters.local_csv_inspector import (
    LocalCsvArtifactInspector,
)
from factoryflow_batch.adapters.local_filesystem_landing import (
    LocalFilesystemLandingStore,
)
from factoryflow_batch.adapters.manifest_json import (
    batch_manifest_from_mapping,
    batch_manifest_to_mapping,
    dumps_batch_manifest,
    loads_batch_manifest,
)
from factoryflow_batch.adapters.postgres_batch_registry import (
    PostgresBatchRegistry,
)
from factoryflow_batch.adapters.postgres_migrations import (
    MigrationExecutionReport,
    PostgresMigrationRunner,
    SqlMigration,
    discover_sql_migrations,
)
from factoryflow_batch.adapters.postgres_outbox_store import (
    PostgresOutboxStore,
)
from factoryflow_batch.adapters.runtime import (
    SystemUtcClock,
    UuidLeaseIdGenerator,
)

__all__ = [
    "ConfluentKafkaEventPublisher",
    "CsvArtifactValidationError",
    "ImmutableLandingConflictError",
    "KafkaPublisherConfiguration",
    "KafkaPublisherConfigurationError",
    "LocalAdapterError",
    "LocalArtifactAccessError",
    "LocalArtifactIntegrityError",
    "LocalCsvArtifactInspector",
    "LocalFilesystemLandingStore",
    "MigrationExecutionReport",
    "PostgresBatchRegistry",
    "PostgresBatchRegistryDataError",
    "PostgresBatchRegistryError",
    "PostgresMigrationDiscoveryError",
    "PostgresMigrationDriftError",
    "PostgresMigrationError",
    "PostgresMigrationExecutionError",
    "PostgresMigrationRunner",
    "PostgresOutboxStore",
    "PostgresOutboxStoreDataError",
    "PostgresOutboxStoreError",
    "SqlMigration",
    "SystemUtcClock",
    "UuidLeaseIdGenerator",
    "batch_manifest_from_mapping",
    "batch_manifest_to_mapping",
    "discover_sql_migrations",
    "dumps_batch_manifest",
    "loads_batch_manifest",
]
