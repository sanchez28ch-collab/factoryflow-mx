-- FactoryFlow P01: durable batch registry and transactional outbox.
-- PostgreSQL 17 or newer.

SELECT pg_advisory_xact_lock(7001, 1);

CREATE SCHEMA IF NOT EXISTS ingestion;

CREATE TABLE IF NOT EXISTS ingestion.batch_manifests (
    batch_id UUID PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    manifest_version TEXT NOT NULL,
    source_system TEXT NOT NULL,
    dataset TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    logical_date DATE NOT NULL,
    source_file_name TEXT NOT NULL,
    landing_uri TEXT NOT NULL,
    content_type TEXT NOT NULL,
    extracted_at TIMESTAMPTZ NOT NULL,
    landed_at TIMESTAMPTZ NOT NULL,
    record_count BIGINT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    sha256 TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT batch_manifests_idempotency_key_uq
        UNIQUE (idempotency_key),

    CONSTRAINT batch_manifests_landing_uri_uq
        UNIQUE (landing_uri),

    CONSTRAINT batch_manifests_idempotency_key_ck
        CHECK (idempotency_key ~ '^[a-f0-9]{64}$'),

    CONSTRAINT batch_manifests_manifest_version_ck
        CHECK (manifest_version ~ '^[0-9]+\.[0-9]+\.[0-9]+$'),

    CONSTRAINT batch_manifests_source_system_ck
        CHECK (
            char_length(source_system) BETWEEN 3 AND 63
            AND source_system ~ '^[a-z][a-z0-9_]*$'
        ),

    CONSTRAINT batch_manifests_dataset_ck
        CHECK (
            char_length(dataset) BETWEEN 3 AND 63
            AND dataset ~ '^[a-z][a-z0-9_]*$'
        ),

    CONSTRAINT batch_manifests_schema_version_ck
        CHECK (schema_version ~ '^[0-9]+\.[0-9]+\.[0-9]+$'),

    CONSTRAINT batch_manifests_source_file_name_ck
        CHECK (
            source_file_name <> ''
            AND source_file_name = btrim(source_file_name)
            AND source_file_name NOT IN ('.', '..')
            AND strpos(source_file_name, '/') = 0
            AND strpos(source_file_name, E'\\') = 0
        ),

    CONSTRAINT batch_manifests_landing_uri_ck
        CHECK (
            landing_uri = btrim(landing_uri)
            AND landing_uri ~ '^(file:///|gs://[^/]+/.+)'
        ),

    CONSTRAINT batch_manifests_content_type_ck
        CHECK (
            content_type = btrim(content_type)
            AND strpos(content_type, '/') > 1
        ),

    CONSTRAINT batch_manifests_temporal_order_ck
        CHECK (landed_at >= extracted_at),

    CONSTRAINT batch_manifests_record_count_ck
        CHECK (record_count >= 0),

    CONSTRAINT batch_manifests_file_size_ck
        CHECK (file_size_bytes > 0),

    CONSTRAINT batch_manifests_sha256_ck
        CHECK (sha256 ~ '^[a-f0-9]{64}$')
);

CREATE INDEX IF NOT EXISTS batch_manifests_source_dataset_date_idx
    ON ingestion.batch_manifests (
        source_system,
        dataset,
        logical_date DESC
    );

CREATE INDEX IF NOT EXISTS batch_manifests_sha256_idx
    ON ingestion.batch_manifests (sha256);

CREATE TABLE IF NOT EXISTS ingestion.outbox_events (
    event_id UUID PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    event_version SMALLINT NOT NULL,
    destination_topic TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    headers JSONB NOT NULL DEFAULT '{}'::JSONB,
    occurred_at TIMESTAMPTZ NOT NULL,
    available_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    dead_lettered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT outbox_events_batch_fk
        FOREIGN KEY (aggregate_id)
        REFERENCES ingestion.batch_manifests (batch_id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT,

    CONSTRAINT outbox_events_aggregate_event_uq
        UNIQUE (
            aggregate_type,
            aggregate_id,
            event_type,
            event_version
        ),

    CONSTRAINT outbox_events_aggregate_type_ck
        CHECK (
            aggregate_type = btrim(aggregate_type)
            AND aggregate_type ~ '^[a-z][a-z0-9._-]*$'
        ),

    CONSTRAINT outbox_events_event_type_ck
        CHECK (
            event_type = btrim(event_type)
            AND event_type ~ '^[a-z][a-z0-9._-]*$'
        ),

    CONSTRAINT outbox_events_event_version_ck
        CHECK (event_version > 0),

    CONSTRAINT outbox_events_topic_ck
        CHECK (
            destination_topic = btrim(destination_topic)
            AND destination_topic ~ '^[A-Za-z0-9._-]+$'
        ),

    CONSTRAINT outbox_events_partition_key_ck
        CHECK (
            partition_key <> ''
            AND partition_key = btrim(partition_key)
        ),

    CONSTRAINT outbox_events_payload_ck
        CHECK (jsonb_typeof(payload) = 'object'),

    CONSTRAINT outbox_events_headers_ck
        CHECK (jsonb_typeof(headers) = 'object'),

    CONSTRAINT outbox_events_attempt_count_ck
        CHECK (attempt_count >= 0),

    CONSTRAINT outbox_events_lease_ck
        CHECK (
            (lease_owner IS NULL AND lease_expires_at IS NULL)
            OR
            (
                lease_owner IS NOT NULL
                AND lease_owner <> ''
                AND lease_owner = btrim(lease_owner)
                AND lease_expires_at IS NOT NULL
            )
        ),

    CONSTRAINT outbox_events_terminal_state_ck
        CHECK (
            NOT (
                published_at IS NOT NULL
                AND dead_lettered_at IS NOT NULL
            )
        ),

    CONSTRAINT outbox_events_terminal_lease_ck
        CHECK (
            (
                published_at IS NULL
                AND dead_lettered_at IS NULL
            )
            OR
            (
                lease_owner IS NULL
                AND lease_expires_at IS NULL
            )
        )
);

CREATE INDEX IF NOT EXISTS outbox_events_pending_idx
    ON ingestion.outbox_events (
        available_at,
        created_at,
        event_id
    )
    WHERE published_at IS NULL
      AND dead_lettered_at IS NULL;

CREATE INDEX IF NOT EXISTS outbox_events_expired_lease_idx
    ON ingestion.outbox_events (lease_expires_at)
    WHERE lease_expires_at IS NOT NULL
      AND published_at IS NULL
      AND dead_lettered_at IS NULL;

CREATE INDEX IF NOT EXISTS outbox_events_aggregate_idx
    ON ingestion.outbox_events (
        aggregate_type,
        aggregate_id,
        created_at
    );

CREATE OR REPLACE FUNCTION ingestion.reject_batch_manifest_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION
        'batch manifests are immutable: operation % is not allowed',
        TG_OP
        USING ERRCODE = '55000';

    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS reject_batch_manifest_mutation
    ON ingestion.batch_manifests;

CREATE TRIGGER reject_batch_manifest_mutation
BEFORE UPDATE OR DELETE
ON ingestion.batch_manifests
FOR EACH ROW
EXECUTE FUNCTION ingestion.reject_batch_manifest_mutation();

COMMENT ON TABLE ingestion.batch_manifests IS
    'Immutable registry of successfully landed P01 batch artifacts';

COMMENT ON TABLE ingestion.outbox_events IS
    'Transactional events awaiting delivery to the FactoryFlow event stream';

COMMENT ON COLUMN ingestion.batch_manifests.idempotency_key IS
    'Deterministic SHA-256 identity used to deduplicate ingestion retries';

COMMENT ON COLUMN ingestion.outbox_events.partition_key IS
    'Stable key used to preserve aggregate ordering in the destination topic';

COMMENT ON COLUMN ingestion.outbox_events.lease_expires_at IS
    'Deadline after which an unfinished publisher lease may be reclaimed';

COMMENT ON COLUMN ingestion.outbox_events.dead_lettered_at IS
    'Terminal timestamp for events exhausted by the delivery policy';
