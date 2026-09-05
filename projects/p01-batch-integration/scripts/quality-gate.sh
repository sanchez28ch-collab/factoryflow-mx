#!/usr/bin/env bash
set -Eeuo pipefail

ff_script_dir="$(
  cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &&
  pwd
)"

ff_project_root="$(
  cd -- "${ff_script_dir}/.." &&
  pwd
)"

ff_python="${FF_PYTHON_BIN:-python3}"

if ! "${ff_python}" --version >/dev/null 2>&1; then
  printf 'ERROR: interprete Python no disponible: %s\n' "${ff_python}" >&2
  exit 1
fi

cd "${ff_project_root}"

printf '[1/5] Verificando formato\n'
"${ff_python}" -m ruff format --check src tests

printf '[2/5] Ejecutando analisis estatico\n'
"${ff_python}" -m ruff check src tests

printf '[3/5] Verificando tipado estricto\n'
"${ff_python}" -m mypy src

printf '[4/5] Ejecutando pruebas y cobertura\n'
"${ff_python}" -m pytest \
  --cov=factoryflow_batch \
  --cov-report=term-missing

printf '[5/5] Verificando compilacion\n'
"${ff_python}" -m compileall -q src

printf '\nPASS: P01 Contract Quality Gate completo.\n'
