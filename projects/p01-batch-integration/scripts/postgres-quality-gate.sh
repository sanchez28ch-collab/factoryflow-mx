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

ff_run_gate() {
  printf '[postgres] Applying and verifying versioned migrations\n'

  P01_DATABASE_URL="${P01_TEST_DATABASE_URL}" \
    python3 -m factoryflow_batch.entrypoints.migrate_postgres \
      --migration-directory "${ff_project_dir}/sql/migrations"

  "${ff_script_dir}/quality-gate.sh"
}

if [[ -n "${P01_TEST_DATABASE_URL:-}" ]]; then
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

if [[
  -z "${ff_pg_user}" ||
  -z "${ff_pg_password}" ||
  -z "${ff_pg_port}"
]]; then
  printf 'ERROR: PostgreSQL connection settings could not be resolved.\n' >&2
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
export P01_TEST_DATABASE_URL

unset ff_pg_password

ff_run_gate

unset P01_TEST_DATABASE_URL
