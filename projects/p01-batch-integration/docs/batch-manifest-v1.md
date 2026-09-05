# Batch Manifest v1

## Purpose

The batch manifest is the immutable technical identity of every artifact
received by FactoryFlow P01.

It provides traceability, deduplication, reconciliation and replay without
depending on the original source system remaining available.

## Ownership

- Producer: P01 Batch Integration Hub.
- Consumers: P03 Warehouse, P04 Data Quality and P05 Composer.
- Source of truth: the versioned JSON Schema under `shared/contracts`.
- Encoding: UTF-8 JSON.
- Current version: `1.0.0`.

## Lifecycle

1. P01 discovers an extract.
2. P01 copies it to temporary storage.
3. P01 calculates its size and SHA-256 digest.
4. P01 parses the artifact and obtains its record count.
5. P01 writes the original artifact to immutable landing storage.
6. P01 creates and stores the manifest.
7. The manifest is never modified.
8. Processing states are stored as separate audit events.

## Idempotency

The idempotency key is a SHA-256 digest calculated from:

- Source system.
- Dataset.
- Logical date.
- Schema version.
- Artifact SHA-256 digest.

Receiving the same logical artifact again produces the same idempotency key,
even when a different execution generates a new `batch_id`.

## Invariants

- Timestamps include a timezone and are normalized to UTC.
- `landed_at` cannot be earlier than `extracted_at`.
- Record counts cannot be negative.
- File size must be greater than zero.
- Hashes contain 64 lowercase hexadecimal characters.
- File names cannot contain directory paths.
- Landing URIs use `gs://` in GCP or `file:///` locally.
- Unknown fields are rejected.
- URIs cannot contain credentials or secrets.

## Compatibility

Patch and minor versions remain backward compatible. Removing a field,
changing its meaning or narrowing accepted values requires a new major
contract version.

Every schema change must pass positive, negative and backward-compatibility
tests before it can be merged.
