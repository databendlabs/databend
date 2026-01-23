#!/bin/bash

set -e

BUILD_PROFILE="${BUILD_PROFILE:-debug}"
SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../" || exit

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required for tests/cloud_control_server" >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for Sandbox UDF test" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required for Sandbox UDF test" >&2
  exit 1
fi

S3_ENDPOINT_URL="${S3_ENDPOINT_URL:-http://127.0.0.1:9900}"
S3_ACCESS_KEY_ID="${S3_ACCESS_KEY_ID:-minioadmin}"
S3_SECRET_ACCESS_KEY="${S3_SECRET_ACCESS_KEY:-minioadmin}"
UDF_IMPORT_PRESIGN_EXPIRE_SECS="${UDF_IMPORT_PRESIGN_EXPIRE_SECS:-43200}"
UDF_DOCKER_BASE_IMAGE="python:3.12-slim"
UDF_DOCKER_RUNTIME_IMAGE="databend-udf-runtime:py312"
UDF_QUERY_TIMEOUT_SECS="${UDF_QUERY_TIMEOUT_SECS:-180}"

echo "Cleaning up previous runs"

killall -9 databend-query || true
killall -9 databend-meta || true
rm -rf .databend


echo "Pre-pulling base image"
docker pull "${UDF_DOCKER_BASE_IMAGE}"

echo "Building runtime image"
tmp_dir="$(mktemp -d)"
cat <<EOF > "${tmp_dir}/Dockerfile"
FROM ${UDF_DOCKER_BASE_IMAGE}
WORKDIR /app
RUN python -m pip install --no-cache-dir uv
RUN uv pip install --system databend-udf
EOF
docker build -t "${UDF_DOCKER_RUNTIME_IMAGE}" -f "${tmp_dir}/Dockerfile" "${tmp_dir}"
rm -rf "${tmp_dir}"
UDF_DOCKER_BASE_IMAGE="${UDF_DOCKER_RUNTIME_IMAGE}"

apply_query_settings() {
  local config_file="$1"
  local tmp_file
  tmp_file="$(mktemp)"
  awk '
    BEGIN { added=0 }
    /^\[query\]$/ {
      print
      if (!added) {
        print "enable_udf_sandbox = true"
        print "cloud_control_grpc_server_address = \"http://0.0.0.0:50051\""
        added=1
      }
      next
    }
    /^enable_udf_sandbox[[:space:]]*=/ { next }
    /^cloud_control_grpc_server_address[[:space:]]*=/ { next }
    { print }
    END {
      if (!added) {
        print "[query]"
        print "enable_udf_sandbox = true"
        print "cloud_control_grpc_server_address = \"http://0.0.0.0:50051\""
      }
    }
  ' "$config_file" > "$tmp_file"
  mv "$tmp_file" "$config_file"
}

for node in 1 2 3; do
  CONFIG_FILE="./scripts/ci/deploy/config/databend-query-node-${node}.toml"
  apply_query_settings "$CONFIG_FILE"
done

echo "Starting Databend Meta HA cluster (3 nodes)"
mkdir -p ./.databend/

nohup ./target/${BUILD_PROFILE}/databend-meta -c scripts/ci/deploy/config/databend-meta-node-1.toml >./.databend/meta-1.out 2>&1 &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9191

sleep 1
nohup ./target/${BUILD_PROFILE}/databend-meta -c scripts/ci/deploy/config/databend-meta-node-2.toml >./.databend/meta-2.out 2>&1 &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 28202

sleep 1
nohup ./target/${BUILD_PROFILE}/databend-meta -c scripts/ci/deploy/config/databend-meta-node-3.toml >./.databend/meta-3.out 2>&1 &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 28302

sleep 1
for node in 1 2 3; do
  CONFIG_FILE="./scripts/ci/deploy/config/databend-query-node-${node}.toml"
  PORT="909${node}"
  echo "Starting databend-query node-${node}"
  nohup env RUST_BACKTRACE=1 target/${BUILD_PROFILE}/databend-query -c "$CONFIG_FILE" >./.databend/query-${node}.out 2>&1 &
  python3 scripts/ci/wait_tcp.py --timeout 30 --port "$PORT"
done
python3 scripts/ci/wait_tcp.py --timeout 30 --port 8000

echo "Starting sandbox control mock server"
export UDF_DOCKER_BASE_IMAGE
nohup env PYTHONUNBUFFERED=1 uv run --project tests/cloud_control_server python tests/cloud_control_server/simple_server.py >./.databend/cloud-control.out 2>&1 &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 50051

check_response_error() {
  local response="$1"
  local error_msg
  error_msg=$(echo "$response" | jq -r 'if .state == "Failed" then .error.message else empty end')

  if [ -n "$error_msg" ]; then
    echo "[Test Error] $error_msg" >&2
    exit 1
  fi
}

collect_query_data() {
  local response="$1"
  local data next_uri page
  local start_ts elapsed
  data=$(echo "$response" | jq -c '.data // []')
  next_uri=$(echo "$response" | jq -r '.next_uri // empty')
  start_ts=$(date +%s)

  while [ -n "$next_uri" ]; do
    elapsed=$(( $(date +%s) - start_ts ))
    if [ "$elapsed" -ge "$UDF_QUERY_TIMEOUT_SECS" ]; then
      echo "[Test Error] query timed out after ${UDF_QUERY_TIMEOUT_SECS}s" >&2
      exit 1
    fi

    response=$(curl -s -u root: "http://localhost:8000${next_uri}")
    check_response_error "$response"
    page=$(echo "$response" | jq -c '.data // []')
    data=$(jq -c --argjson acc "$data" --argjson page "$page" -n '$acc + $page')
    next_uri=$(echo "$response" | jq -r '.next_uri // empty')
    if [ -n "$next_uri" ] && [ "$page" = "[]" ]; then
      sleep 0.2
    fi
  done

  echo "$data"
}

echo "Preparing stage imports"
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9900

create_stage_sql=$(cat <<SQL
CREATE STAGE IF NOT EXISTS udf_import_stage
URL = 's3://testbucket/udf-imports/'
CONNECTION = (
  access_key_id = '${S3_ACCESS_KEY_ID}',
  secret_access_key = '${S3_SECRET_ACCESS_KEY}',
  endpoint_url = '${S3_ENDPOINT_URL}'
);
SQL
)
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  -H 'Content-Type: application/json' \
  -d "$(jq -n --arg sql "$create_stage_sql" '{sql: $sql}')" )
check_response_error "$response"

tmp_dir="$(mktemp -d)"
helper_file="${tmp_dir}/helper.py"
cat <<'PY' > "$helper_file"
def add_one(x: int) -> int:
    return x + 1
PY

upload_response=$(curl -s -u root: -XPUT "http://localhost:8000/v1/upload_to_stage" \
  -H "x-databend-stage-name: udf_import_stage" \
  -H "x-databend-relative-path: imports" \
  -F "file=@${helper_file}")
upload_state=$(echo "$upload_response" | jq -r '.state')
if [ "$upload_state" != "SUCCESS" ]; then
  echo "[Test Error] upload to stage failed: $upload_response" >&2
  exit 1
fi

echo "Creating python UDF"
create_udf_sql=$(cat <<'SQL'
CREATE OR REPLACE FUNCTION add_one(INT)
RETURNS INT
LANGUAGE PYTHON
IMPORTS = ('@udf_import_stage/imports/helper.py')
PACKAGES = ()
HANDLER = 'add_one'
AS $$
import helper

def add_one(x: int) -> int:
    return helper.add_one(x)
$$
SQL
)
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  -H 'Content-Type: application/json' \
  -d "$(jq -n --arg sql "$create_udf_sql" '{sql: $sql}')" )
check_response_error "$response"

echo "Executing python UDF"
select_udf_sql="SELECT add_one(1) AS result"
response=$(curl -s -u root: -XPOST "http://localhost:8000/v1/query" \
  -H 'Content-Type: application/json' \
  -d "$(jq -n \
    --arg sql "$select_udf_sql" \
    --arg presign_secs "$UDF_IMPORT_PRESIGN_EXPIRE_SECS" \
    '{sql: $sql, settings: {udf_cloud_import_presign_expire_secs: $presign_secs}}')" )
check_response_error "$response"

actual=$(collect_query_data "$response")
expected='[["2"]]'

if [ "$actual" = "$expected" ]; then
  echo "✅ Query result matches expected"
else
  echo "❌ Mismatch"
  echo "Expected: $expected"
  echo "Actual  : $actual"
  exit 1
fi

echo "✅ Passed"
