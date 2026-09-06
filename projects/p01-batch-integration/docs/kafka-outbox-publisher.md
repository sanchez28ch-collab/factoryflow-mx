# Kafka transactional-outbox publisher

## Purpose

This capability delivers P01 batch-ingestion events from the PostgreSQL
transactional outbox to Kafka.

It connects the durable write path created during batch registration with the
versioned `factoryflow.batch.ingested.v1` Kafka topic.

The implementation includes:

- Bounded PostgreSQL outbox leasing.
- Concurrent-worker exclusion with `FOR UPDATE SKIP LOCKED`.
- Fenced state transitions using event and lease identifiers.
- Deterministic retry backoff.
- Terminal dead-letter marking.
- Idempotent Kafka producer configuration.
- Confirmed broker acknowledgements.
- Continuous and single-cycle execution modes.
- Structured, non-sensitive operational output.
- Integration tests against real PostgreSQL and Kafka services.

## Delivery model

The publisher provides at-least-once delivery.

The PostgreSQL transaction and Kafka acknowledgement cannot participate in one
shared atomic transaction. A process failure can occur after Kafka confirms a
message but before PostgreSQL records `published_at`.

In that failure window, the lease eventually expires and another worker may
publish the same event again.

Therefore:

- `event_id` is stable for the lifetime of the event.
- Every published message contains `event_id` in its JSON payload.
- Every published message contains `event_id` as a Kafka header.
- Consumers must make event processing idempotent.
- Consumers must deduplicate durable side effects by `event_id`.
- The system does not claim end-to-end exactly-once processing.

Kafka producer idempotence reduces duplicates caused by producer retries. It
does not eliminate duplicates caused by a database acknowledgement failure
after successful broker delivery.

## Publication sequence

One publication cycle performs these operations:

1. Generate a unique lease identifier.
2. Select an available bounded batch from PostgreSQL.
3. Lock candidates with `FOR UPDATE SKIP LOCKED`.
4. Assign the lease and increment `attempt_count`.
5. Serialize the stored JSON payload deterministically.
6. Publish each event to its configured Kafka topic.
7. Poll until Kafka confirms success or the delivery timeout expires.
8. Validate the returned topic, partition and offset.
9. Persist a published, rescheduled or dead-lettered outcome.

## PostgreSQL leasing

A row is claimable only when:

- `published_at` is null.
- `dead_lettered_at` is null.
- `available_at` is not later than the claim time.
- It has no lease or its existing lease has expired.

Every mutation is fenced by both `event_id` and `lease_id`. A worker that loses
ownership cannot acknowledge, reschedule or dead-letter an event using a stale
lease.

Concurrent workers can operate without publishing the same active lease
simultaneously.

## Retry policy

Retryable publication failures are rescheduled with deterministic capped
exponential equal jitter.

The policy controls:

- Maximum batch size.
- Lease duration.
- Maximum publication attempts.
- Base retry delay.
- Maximum retry delay.

The jitter value is derived from the event identifier and attempt count. This
distributes retry load while keeping tests reproducible.

The store persists fixed error codes instead of raw diagnostics:

- `publisher_retryable`
- `publisher_non_retryable`
- `attempt_limit_reached`
- `attempt_limit_exceeded`

Raw exception messages, payloads and credentials are not written to the outbox
error field.

## Dead-letter behavior

Events are dead-lettered after a nonretryable failure or after reaching the
configured attempt limit.

Dead-lettered events are excluded from future claims. Replay is not automatic;
it requires an audited procedure that verifies downstream idempotency.

## Kafka producer guarantees

`ConfluentKafkaEventPublisher` configures librdkafka with:

- `enable.idempotence=true`
- `acks=all`
- Maximum producer retries.
- At most five in-flight requests per connection.
- Topic auto-creation disabled.
- Zstandard compression by default.
- Bounded request and delivery timeouts.
- Broker-confirmed delivery callbacks.

A publication succeeds only after Kafka returns the expected topic, a
nonnegative partition and a nonnegative offset.

The publisher rejects empty topics, keys or payloads, payloads larger than
1,000,000 bytes, oversized headers and invalid broker acknowledgements.

## Topic contract

The local and CI topic is:

`factoryflow.batch.ingested.v1`

It is created explicitly with three partitions. The partition key is the batch
aggregate identifier.

Automatic topic creation remains disabled to prevent accidental unversioned
topics and configuration drift.

## Operational command

The installed command is `factoryflow-p01-publish-outbox`.

Run one bounded cycle with:

`factoryflow-p01-publish-outbox --run-once`

Run continuously with:

`factoryflow-p01-publish-outbox`

Continuous mode waits after an idle cycle and reacts to `SIGINT` and `SIGTERM`.
It restores signal handlers and closes the PostgreSQL connection pool during a
controlled shutdown.

## Environment configuration

Required environment variables:

- `P01_DATABASE_URL`
- `P01_KAFKA_BOOTSTRAP_SERVERS`

Database and Kafka endpoints are not accepted as command-line arguments.

Optional environment variables:

- `P01_OUTBOX_WORKER_ID`
- `P01_OUTBOX_BATCH_SIZE`
- `P01_OUTBOX_LEASE_SECONDS`
- `P01_OUTBOX_MAX_ATTEMPTS`
- `P01_OUTBOX_RETRY_BASE_MS`
- `P01_OUTBOX_RETRY_MAX_MS`
- `P01_OUTBOX_IDLE_WAIT_MS`
- `P01_DATABASE_CONNECT_TIMEOUT_SECONDS`
- `P01_KAFKA_DELIVERY_TIMEOUT_MS`
- `P01_KAFKA_REQUEST_TIMEOUT_MS`
- `P01_KAFKA_POLL_INTERVAL_MS`
- `P01_KAFKA_COMPRESSION_TYPE`

Unsafe identifiers, invalid durations and excessive limits are rejected before
opening the PostgreSQL connection pool.

## Exit codes

Stable process exit codes:

- `0`: successful cycle or controlled shutdown.
- `2`: invalid or unsafe configuration.
- `3`: PostgreSQL connection or persistence failure.
- `4`: publication consistency failure.

Retryable broker failures normally produce a successful cycle report with a
positive `rescheduled` count because the retry decision was durably persisted.

## Operational output

Every cycle emits one compact JSON object containing its timestamp, status and
the claimed, published, rescheduled and dead-lettered counters.

Output never includes database URLs, passwords, event payloads, Kafka
diagnostic text or PostgreSQL diagnostic text.

## Quality evidence

The integrated quality gate validates:

- Formatting, static analysis and strict typing.
- Python and Bash compilation or syntax.
- Policy, backoff and application-service behavior.
- Adversarial persistence and consistency failures.
- PostgreSQL leasing and fenced transitions.
- Kafka configuration and delivery callbacks.
- Real broker publication and consumption.
- Complete PostgreSQL-to-Kafka delivery.
- PostgreSQL acknowledgement after broker confirmation.
- Credential and diagnostic redaction.
- Controlled continuous-worker shutdown.

The local gate and GitHub Actions use the repository's PostgreSQL 17.11 and
Kafka 4.3.1 Docker Compose platform.

## Security boundary

The local and CI adapter uses Kafka `PLAINTEXT` because the Docker Compose
platform is bound to the local host and isolated from untrusted networks.

This configuration must not be used across an untrusted or production network.

Production requires authenticated TLS, managed secrets, Kafka ACLs,
certificate validation and credential rotation without exposing secrets in
command-line arguments or logs.

## Remaining operational work

The following capabilities remain outside this delivery:

- Authenticated TLS and Kafka ACL configuration.
- Automated dead-letter replay authorization.
- PostgreSQL-to-Kafka reconciliation.
- Consumer-side deduplication implementation.
- Metrics export and alerting dashboards.
- Multi-broker production topology.
