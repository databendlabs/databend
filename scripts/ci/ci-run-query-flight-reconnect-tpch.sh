#!/usr/bin/env bash
# Copyright 2020-2026 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -euo pipefail

export STORAGE_TYPE=s3
export STORAGE_S3_BUCKET=testbucket
export STORAGE_S3_ROOT=admin
export STORAGE_S3_ENDPOINT_URL=http://127.0.0.1:9900
export STORAGE_S3_ACCESS_KEY_ID=minioadmin
export STORAGE_S3_SECRET_ACCESS_KEY=minioadmin
export STORAGE_ALLOW_INSECURE=true

readonly BUILD_PROFILE="${BUILD_PROFILE:-debug}"
readonly SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
readonly REPO_PATH="$(cd "$SCRIPT_PATH/../.." >/dev/null 2>&1 && pwd)"
readonly TPCH_DATA_PATH=/tmp/tpch_1

python3 -m pip install --quiet mysql-connector-python requests
sudo apt-get update -yq
sudo apt-get install -yq iproute2 iptables lsof

cd "$REPO_PATH"
./scripts/ci/deploy/databend-query-cluster-3-nodes.sh

rm -rf -- "$TPCH_DATA_PATH"
bash tests/sqllogictests/scripts/prepare_tpch_data.sh tpch_test 1

python3 tests/query-flight-reconnect/test_tpch_reconnect.py \
	--sqllogictests "target/${BUILD_PROFILE}/databend-sqllogictests" \
	--tpch-suite tests/sqllogictests/suites/tpch/queries.test \
	--operation-log .databend/tpch-flight-reconnect/operations.log \
	--repo-dir "$REPO_PATH"
