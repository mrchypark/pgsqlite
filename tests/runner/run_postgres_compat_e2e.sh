#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

IMAGE_NAME="pgsqlite-e2e-local-postgres-compat"
CONTAINER_NAME="pgsqlite-e2e-postgres-compat"
VOLUME_NAME="pgsqlite-e2e-data-postgres-compat"
HOST_PORT="55432"

cleanup() {
  docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
  docker volume rm "${VOLUME_NAME}" >/dev/null 2>&1 || true
}

expect_output_contains() {
  local output="$1"
  local expected="$2"
  if [[ "${output}" != *"${expected}"* ]]; then
    echo "[e2e] Expected output to contain '${expected}', got: ${output}" >&2
    exit 1
  fi
}

trap cleanup EXIT

echo "[e2e] Building image: ${IMAGE_NAME}"
docker rmi -f "${IMAGE_NAME}" >/dev/null 2>&1 || true
docker build -t "${IMAGE_NAME}" "${ROOT_DIR}" >/dev/null

cleanup

echo "[e2e] Starting container: ${CONTAINER_NAME}"
docker run -d --name "${CONTAINER_NAME}" -p "${HOST_PORT}:5432" \
  -v "${VOLUME_NAME}:/var/lib/postgresql/data" \
  "${IMAGE_NAME}" \
  sh -lc "set -eu; mkdir -p /var/lib/postgresql/data; \
    pgsqlite --migrate --database /var/lib/postgresql/data --pragma-journal-mode WAL --default-database default; \
    exec pgsqlite --database /var/lib/postgresql/data --pragma-journal-mode WAL --default-database default" \
  >/dev/null

echo "[e2e] Waiting for ready"
for _ in $(seq 1 30); do
  if docker run --rm -e PGPASSWORD=postgres postgres:16 \
      pg_isready -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo "[e2e] Smoke: information_schema.schemata count"
docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "select count(*) from information_schema.schemata;" \
  >/dev/null

echo "[e2e] Text bool: SELECT EXISTS should return t/f"
exists_out="$(
  docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
    -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
    -v ON_ERROR_STOP=1 \
    -tAc "select exists(select 1);"
)"
exists_out="$(echo "${exists_out}" | tr -d '\r\n')"
if [[ "${exists_out}" != "t" ]]; then
  echo "[e2e] Expected SELECT EXISTS output to be 't', got: ${exists_out}" >&2
  exit 1
fi

echo "[e2e] Schema: create schema foo and verify reflected"
docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "create schema foo;" \
  >/dev/null

foo_schema_out="$(
  docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
    -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
    -v ON_ERROR_STOP=1 \
    -tAc "select schema_name from information_schema.schemata where schema_name = 'foo';"
)"
expect_output_contains "${foo_schema_out}" "foo"

echo "[e2e] Session: SET search_path=foo; current_schema() should return foo"
current_schema_out="$(
  docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
    -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
    -v ON_ERROR_STOP=1 \
    -tAc "set search_path=foo; select current_schema();"
)"
expect_output_contains "${current_schema_out}" "foo"

echo "[e2e] Session: SHOW search_path should return foo"
show_search_path_out="$(
  docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
    -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
    -v ON_ERROR_STOP=1 \
    -tAc "set search_path=foo; show search_path;"
)"
expect_output_contains "${show_search_path_out}" "foo"

echo "[e2e] Session: current_setting('search_path') should return foo"
current_setting_out="$(
  docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
    -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
    -v ON_ERROR_STOP=1 \
    -tAc "set search_path=foo; select current_setting('search_path');"
)"
expect_output_contains "${current_setting_out}" "foo"

echo "[e2e] Compatibility: SELECT * FROM current_schema() should return foo"
current_schema_star_out="$(
  docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
    -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
    -v ON_ERROR_STOP=1 \
    -tAc "set search_path=foo; select * from current_schema();"
)"
expect_output_contains "${current_schema_star_out}" "foo"

echo "[e2e] Session: set_config('search_path','bar',false) should affect current_schema()"
docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
  -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
  -v ON_ERROR_STOP=1 \
  -c "create schema bar;" \
  >/dev/null

set_config_schema_out="$(
  docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
    -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
    -v ON_ERROR_STOP=1 \
    -tAc "select set_config('search_path','bar',false); select current_schema();"
)"
expect_output_contains "${set_config_schema_out}" "bar"

set_config_show_out="$(
  docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
    -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
    -v ON_ERROR_STOP=1 \
    -tAc "select set_config('search_path','bar',false); show search_path;"
)"
expect_output_contains "${set_config_show_out}" "bar"

echo "[e2e] SQL PREPARE/EXECUTE/DEALLOCATE (simple query)"
prepare_out="$(
  docker run --rm -e PGPASSWORD=postgres postgres:16 psql \
    -h host.docker.internal -p "${HOST_PORT}" -U postgres -d default \
    -v ON_ERROR_STOP=1 \
    -tAc "prepare p1(int4) as select \$1::int4 + 1; execute p1(41); deallocate p1;"
)"
expect_output_contains "${prepare_out}" "42"

echo "[e2e] OK"
