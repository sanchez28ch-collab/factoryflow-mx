#!/usr/bin/env bash
# PostgreSQL variables intentionally expand inside the container shell.
# shellcheck disable=SC2016
set -Eeuo pipefail

ff_script_dir="$(
  cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
  pwd
)"
ff_project_dir="$(
  cd -- "${ff_script_dir}/.."
  pwd
)"
ff_repo_root="$(
  git -C "${ff_project_dir}" rev-parse --show-toplevel
)"

readonly ff_script_dir
readonly ff_project_dir
readonly ff_repo_root

if [[ -x "${ff_repo_root}/.venv/bin/python3" ]]; then
  export PATH="${ff_repo_root}/.venv/bin:${PATH}"
fi

ff_prepare_kafka_topic() {
  printf '[kafka] Creating and verifying versioned integration topic\n'

  python3 - <<'PY'
import os

from confluent_kafka.admin import AdminClient, NewTopic

bootstrap_servers = os.environ[
    "P01_TEST_KAFKA_BOOTSTRAP_SERVERS"
]
topic = "factoryflow.batch.ingested.v1"

admin = AdminClient(
    {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "factoryflow-p01-quality-gate",
        "security.protocol": "PLAINTEXT",
        "allow.auto.create.topics": False,
    }
)
metadata = admin.list_topics(timeout=15)

if topic not in metadata.topics:
    future = admin.create_topics(
        [
            NewTopic(
                topic,
                num_partitions=3,
                replication_factor=1,
            )
        ],
        operation_timeout=10,
        request_timeout=15,
    )[topic]
    future.result(timeout=15)

metadata = admin.list_topics(topic=topic, timeout=15)
topic_metadata = metadata.topics.get(topic)

if topic_metadata is None:
    raise RuntimeError("The Kafka integration topic was not found.")

if topic_metadata.error is not None:
    raise RuntimeError("Kafka returned invalid topic metadata.")

if len(topic_metadata.partitions) != 3:
    raise RuntimeError(
        "The Kafka integration topic must contain three partitions."
    )

print(
    "PASS: Kafka topic is available with "
    f"{len(topic_metadata.partitions)} partitions."
)
PY
}

ff_run_gate() {
  printf '[postgres] Applying and verifying versioned migrations\n'

  P01_DATABASE_URL="${P01_TEST_DATABASE_URL}" \
    python3 -m factoryflow_batch.entrypoints.migrate_postgres \
      --migration-directory "${ff_project_dir}/sql/migrations"

  ff_prepare_kafka_topic
  "${ff_script_dir}/quality-gate.sh"
}

if [[
  -n "${P01_TEST_DATABASE_URL:-}" ||
  -n "${P01_TEST_KAFKA_BOOTSTRAP_SERVERS:-}"
]]; then
  if [[
    -z "${P01_TEST_DATABASE_URL:-}" ||
    -z "${P01_TEST_KAFKA_BOOTSTRAP_SERVERS:-}"
  ]]; then
    printf '%s\n' \
      'ERROR: both P01_TEST_DATABASE_URL and' \
      'P01_TEST_KAFKA_BOOTSTRAP_SERVERS are required.' >&2
    exit 1
  fi

  ff_run_gate
  exit 0
fi

ff_env_file="${ff_repo_root}/infrastructure/local/.env"
ff_compose_file="${ff_repo_root}/infrastructure/local/compose.yaml"
ff_test_database="factoryflow_p01_test"

readonly ff_env_file
readonly ff_compose_file
readonly ff_test_database

if [[ ! -f "${ff_env_file}" ]]; then
  printf 'ERROR: local Compose environment file does not exist: %s\n' \
    "${ff_env_file}" >&2
  exit 1
fi

ff_compose=(
  docker compose
  --env-file "${ff_env_file}"
  --file "${ff_compose_file}"
)

if ! "${ff_compose[@]}" exec -T postgres true >/dev/null 2>&1; then
  printf 'ERROR: the local PostgreSQL container is not running.\n' >&2
  exit 1
fi

if ! "${ff_compose[@]}" exec -T kafka true >/dev/null 2>&1; then
  printf 'ERROR: the local Kafka container is not running.\n' >&2
  exit 1
fi

printf '[postgres] Preparing isolated integration-test database\n'

ff_database_exists="$(
  printf '%s\n' \
    "SELECT 1 FROM pg_database" \
    "WHERE datname = '${ff_test_database}';" |
    "${ff_compose[@]}" exec -T postgres sh -ec '
      psql \
        --username "$POSTGRES_USER" \
        --dbname postgres \
        --tuples-only \
        --no-align
    ' |
    tr -d '[:space:]'
)"

if [[ "${ff_database_exists}" != "1" ]]; then
  "${ff_compose[@]}" exec -T postgres sh -ec '
    createdb \
      --username "$POSTGRES_USER" \
      factoryflow_p01_test
  '
fi

ff_pg_user="$(
  "${ff_compose[@]}" exec -T postgres \
    printenv POSTGRES_USER |
    tr -d '\r\n'
)"
ff_pg_password="$(
  "${ff_compose[@]}" exec -T postgres \
    printenv POSTGRES_PASSWORD |
    tr -d '\r\n'
)"
ff_pg_port="$(
  "${ff_compose[@]}" port postgres 5432 |
    sed -E 's/.*:([0-9]+)$/\1/'
)"
ff_kafka_port="$(
  "${ff_compose[@]}" port kafka 9092 |
    sed -E 's/.*:([0-9]+)$/\1/'
)"

if [[
  -z "${ff_pg_user}" ||
  -z "${ff_pg_password}" ||
  -z "${ff_pg_port}" ||
  -z "${ff_kafka_port}"
]]; then
  printf 'ERROR: integration connection settings could not be resolved.\n' >&2
  exit 1
fi

P01_TEST_DATABASE_URL="$(
  FF_PG_USER="${ff_pg_user}" \
  FF_PG_PASSWORD="${ff_pg_password}" \
  FF_PG_PORT="${ff_pg_port}" \
  python3 -c '
import os
from psycopg.conninfo import make_conninfo

print(
    make_conninfo(
        host="127.0.0.1",
        port=os.environ["FF_PG_PORT"],
        dbname="factoryflow_p01_test",
        user=os.environ["FF_PG_USER"],
        password=os.environ["FF_PG_PASSWORD"],
    )
)
'
)"
P01_TEST_KAFKA_BOOTSTRAP_SERVERS="localhost:${ff_kafka_port}"

export P01_TEST_DATABASE_URL
export P01_TEST_KAFKA_BOOTSTRAP_SERVERS

unset ff_pg_password

ff_run_gate

unset P01_TEST_DATABASE_URL
unset P01_TEST_KAFKA_BOOTSTRAP_SERVERS
