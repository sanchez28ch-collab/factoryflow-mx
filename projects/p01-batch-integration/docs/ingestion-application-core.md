# P01 Batch Ingestion Application Core

## Purpose

The application core coordinates the ingestion of immutable batch artifacts
without depending on Google Cloud Storage, PostgreSQL, local filesystems or a
specific identifier implementation.

Infrastructure integrations implement explicit ports. This keeps business
rules independently testable and allows the same workflow to run locally and
in GCP.

## Execution flow

1. Validate the ingestion command before external side effects.
2. Inspect the source artifact using a streaming adapter.
3. Calculate the business idempotency key.
4. Query the batch registry for an existing manifest.
5. Return the existing manifest when the batch was already processed.
6. Build a deterministic raw-zone object key.
7. Land the artifact using immutable storage semantics.
8. Compare the landed size and SHA-256 against the inspected source.
9. Build the immutable versioned batch manifest.
10. Register the manifest using an atomic insert-or-return operation.
11. Return whether the request created or deduplicated the batch.

## Application ports

| Port | Responsibility |
| --- | --- |
| `ArtifactInspector` | Stream the source and calculate metadata, row count, size and SHA-256. |
| `LandingStore` | Persist the artifact under a deterministic object key. |
| `BatchRegistry` | Enforce the unique business-idempotency constraint. |
| `BatchIdGenerator` | Generate globally unique non-zero batch identifiers. |

## Idempotency

The business key is derived from:

- Source system.
- Dataset.
- Logical date.
- Schema version.
- Artifact SHA-256.

The physical object key is derived from that business key. Retrying the same
artifact therefore targets the same storage location.

The registry must enforce uniqueness atomically. An optimistic lookup reduces
unnecessary storage operations, while `register_if_absent` resolves concurrent
requests that pass the initial lookup simultaneously.

## Integrity guarantees

P01 rejects a landing receipt when either of these values differs from the
source inspection:

- SHA-256 digest.
- File size in bytes.

The manifest is registered only after both checks pass.

## Failure semantics

Invalid commands fail before inspection.

Invalid inspected metadata fails before landing.

Integrity mismatches fail before registry mutation.

A registry response containing another idempotency key is treated as a
consistency failure.

A concurrent duplicate returns the manifest that won the atomic registration.

## Security boundaries

Source URIs accept only `file` and `gs` schemes.

Query parameters and URI fragments are rejected to prevent credentials or
mutable request information from becoming part of ingestion commands.

Remote hosts are rejected for `file` URIs.

Source file names cannot contain path traversal components.

## Infrastructure adapters planned next

- Streaming CSV inspector.
- Immutable local filesystem landing store.
- UUIDv7-compatible identifier adapter.
- PostgreSQL registry with a unique idempotency constraint.
- Transactional outbox for downstream ingestion events.
- GCS landing adapter using object-generation preconditions.
