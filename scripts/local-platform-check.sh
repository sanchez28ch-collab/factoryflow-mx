#!/usr/bin/env bash

set -Eeuo pipefail

ff_script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ff_project_root="$(cd -- "${ff_script_dir}/.." && pwd)"
ff_local_dir="${ff_project_root}/infrastructure/local"
ff_env_file="${ff_local_dir}/.env"
ff_compose_file="${ff_local_dir}/compose.yaml"

if [[ ! -f "${ff_env_file}" ]]; then
  printf 'FAIL: missing %s\n' "${ff_env_file}" >&2
  exit 1
fi

set -a
# The local env file path is resolved and validated above.
# shellcheck disable=SC1090
source "${ff_env_file}"
set +a

ff_compose=(
  docker compose
  --env-file "${ff_env_file}"
  --file "${ff_compose_file}"
)

printf '\n[1/4] Validating Compose configuration\n'
"${ff_compose[@]}" config --quiet

printf '[2/4] Checking container state\n'
"${ff_compose[@]}" ps

printf '[3/4] Checking PostgreSQL and required schemas\n'
"${ff_compose[@]}" exec -T postgres \
  pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}"

ff_schema_count="$(
  "${ff_compose[@]}" exec -T postgres \
    psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -Atc \
    "SELECT COUNT(*) FROM information_schema.schemata
     WHERE schema_name IN ('operational','ingestion','quality','audit');"
)"

if [[ "${ff_schema_count}" != "4" ]]; then
  printf 'FAIL: expected 4 schemas, found %s\n' "${ff_schema_count}" >&2
  exit 1
fi

printf '[4/4] Checking Kafka broker\n'
"${ff_compose[@]}" exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list >/dev/null

printf '\nPASS: FactoryFlow local platform is healthy.\n'
