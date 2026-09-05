# PostgreSQL batch registry and transactional outbox

## Purpose

This capability provides durable and idempotent PostgreSQL registration for
P01 batch ingestion.

Every newly accepted batch manifest and its corresponding integration event
are written in the same database transaction. A manifest cannot be committed
without its outbox event, and an event cannot be committed without its
manifest.

The implementation supports continuously operating ingestion workloads,
concurrent retries and controlled production deployments.

## Components

### PostgreSQL batch registry

`PostgresBatchRegistry` implements the P01 `BatchRegistry` port.

It provides:

- Lookup by deterministic idempotency key.
- Atomic registration of immutable batch manifests.
- Safe handling of concurrent duplicate submissions.
- Validation of objects reconstructed from persisted data.
- Controlled exceptions without database-detail leakage.
- Transactional creation of one outbox event per accepted batch.

### Immutable batch manifests

`ingestion.batch_manifests` stores the permanent ingestion record.

PostgreSQL enforces:

- UUID primary identity.
- Unique idempotency key.
- Unique immutable landing URI.
- Lowercase SHA-256 values.
- Valid manifest and schema versions.
- Valid source and dataset identifiers.
- Nonnegative record counts.
- Positive artifact sizes.
- Consistent extraction and landing timestamps.
- Rejection of updates and deletes through a database trigger.

Corrections require a new manifest and batch identifier. Historical manifests
are never rewritten.

### Transactional outbox

`ingestion.outbox_events` stores events that must later be delivered to Kafka.

Each `factoryflow.batch.ingested` event contains:

- A deterministic event UUID.
- The aggregate type and batch identifier.
- Event type and version.
- Destination topic and partition key.
- A JSONB payload conforming to the versioned event contract.
- Contract-identifying headers.
- Availability, lease, attempt and publication state.
- Dead-letter and failure information.

The database prevents duplicate aggregate-event combinations.

## Transaction boundary

These operations commit or roll back together:

1. Insert the immutable batch manifest.
2. Detect an idempotent or concurrent winner.
3. Create the versioned outbox event for a new manifest.
4. Return the persisted registration.

If outbox creation fails, the manifest insertion is rolled back.

## Versioned event contract

The event contract is stored at:

`shared/contracts/jsonschema/batch/batch-ingested-v1.schema.json`

Its identifier is:

`urn:factoryflow:events:batch-ingested:1.0.0`

The envelope requires event identity, type, version, occurrence time, aggregate
identity and the nested batch-manifest data.

The `data` property references the versioned batch-manifest contract. Contract
tests validate both the envelope and its nested manifest.

## Versioned migrations

SQL migrations are stored in:

`projects/p01-batch-integration/sql/migrations`

Migration filenames use `NNN_lowercase_description.sql`.

`PostgresMigrationRunner` guarantees:

- Regular UTF-8 SQL artifacts only.
- Rejection of invalid names, empty files and symbolic links.
- Deterministic numeric ordering.
- SHA-256 checksums for every migration.
- PostgreSQL advisory locking against concurrent execution.
- Version tracking in `audit.schema_migrations`.
- Checksum verification before skipping applied migrations.
- Drift detection for modified or missing historical files.
- Atomic rollback of the complete pending migration batch.

Applied migration files must never be edited. Corrections require a new
migration version.

## Operational migration command

The installed command is `factoryflow-p01-migrate`.

The database connection is accepted only through `P01_DATABASE_URL`. It is
never accepted as a command-line argument, preventing credentials from
appearing in process listings or shell history.

Supported configuration:

- `P01_DATABASE_URL`: required PostgreSQL connection information.
- `P01_MIGRATION_DIRECTORY`: versioned SQL directory.
- `P01_MIGRATION_COMPONENT`: migration-ledger namespace.
- `P01_DATABASE_CONNECT_TIMEOUT_SECONDS`: connection startup limit.
- `P01_MIGRATION_LOCK_TIMEOUT_MS`: PostgreSQL lock limit.
- `P01_MIGRATION_STATEMENT_TIMEOUT_MS`: statement execution limit.

Stable exit codes:

- `0`: migration state verified successfully.
- `2`: invalid configuration or migration artifact.
- `3`: migration drift detected.
- `4`: connection or database execution failure.

The command emits one structured JSON result. It never includes the database
URL, password or internal PostgreSQL diagnostic text.

## Deployment order

A deployment must:

1. Retrieve the database URL from the authorized secret provider.
2. Execute `factoryflow-p01-migrate`.
3. Require exit code `0`.
4. Start or update the application workloads.
5. Verify registry and outbox health signals.

Application workloads must not start after a nonzero migration exit code.

## Production authorization

Production uses separate identities:

- Migration identity with temporary DDL and ledger permissions.
- Ingestion identity with registry insert and read permissions.
- Publisher identity with outbox lease and publication permissions.
- Operations identity with read-only diagnostic access.

Application identities must not own schemas. Credentials must come from an
authorized secret provider and must never enter the repository.

## Failure response

When migration drift is detected:

1. Stop the deployment.
2. Do not modify the migration ledger.
3. Restore the original historical migration.
4. Introduce corrections through a new migration version.
5. Run the complete quality gate again.

When migration execution fails:

1. Confirm no ledger entry was committed for the failed version.
2. Inspect PostgreSQL logs through the authorized operations channel.
3. Correct only the new unapplied migration.
4. Repeat migration and integration validation.

A growing pending outbox must not be fixed by deleting events. Operators must
inspect publisher availability, expired leases, Kafka connectivity, attempt
counts, last errors and dead-letter state.

## Automated evidence

The quality gate validates:

- JSON Schema contracts and nested event data.
- Atomic manifest and outbox persistence.
- Idempotent and concurrent registration.
- Database immutability constraints.
- Migration checksums and deterministic ordering.
- Drift detection and transactional rollback.
- Concurrent migration execution.
- CLI exit codes and credential redaction.
- Ruff, MyPy, ShellCheck and Python compilation.

## Current boundary

This delivery provides PostgreSQL persistence, controlled migrations and the
transactional outbox write path.

Kafka publication, retry scheduling, dead-letter processing and reconciliation
belong to the next delivery stage.
