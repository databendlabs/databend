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
UDF_IMPORT_PRESIGN_EXPIRE_SECS="${UDF_IMPORT_PRESIGN_EXPIRE_SECS:-259200}"
UDF_DOCKER_BASE_IMAGE="python:3.12-slim"
UDF_DOCKER_RUNTIME_IMAGE="databend-udf-runtime:py312"
UDF_QUERY_TIMEOUT_SECS="${UDF_QUERY_TIMEOUT_SECS:-180}"
UDF_IMPORT_STAGE="${UDF_IMPORT_STAGE:-udf_import_stage}"

echo "Cleaning up previous runs"

killall -9 databend-query || true
killall -9 databend-meta || true
pkill -f "tests/cloud_control_server/simple_server.py" || true
pkill -f "uv run --project tests/cloud_control_server" || true
rm -rf .databend

echo "Pre-pulling base image"
docker pull "${UDF_DOCKER_BASE_IMAGE}"

echo "Building runtime image"
tmp_dir="$(mktemp -d)"
cat <<EOF >"${tmp_dir}/Dockerfile"
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
  ' "$config_file" >"$tmp_file"
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

source "$SCRIPT_PATH/test-sandbox-udf-lib.sh"

UDF_SETTINGS_JSON=$(jq -n --arg presign_secs "$UDF_IMPORT_PRESIGN_EXPIRE_SECS" \
	'{udf_cloud_import_presign_expire_secs: $presign_secs}')

case_scripts=(
	"$SCRIPT_PATH/sandbox_cases/00_prepare_imports.sh"
	"$SCRIPT_PATH/sandbox_cases/10_metadata_imports.sh"
	"$SCRIPT_PATH/sandbox_cases/20_numpy.sh"
  "$SCRIPT_PATH/sandbox_cases/25_zip_whl_egg.sh"
  "$SCRIPT_PATH/sandbox_cases/30_read_stage.sh"
  "$SCRIPT_PATH/sandbox_cases/40_read_archive.sh"
  "$SCRIPT_PATH/sandbox_cases/80_presign_errors.sh"
  "$SCRIPT_PATH/sandbox_cases/90_health_stats.sh"
)

for case_script in "${case_scripts[@]}"; do
	echo "Running $(basename "$case_script")"
	source "$case_script"
done

echo "✅ Passed"
