#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

echo "*************************************"
echo "* Setting STORAGE_TYPE to S3.       *"
echo "*                                   *"
echo "* Please make sure that S3 backend  *"
echo "* is ready, and configured properly.*"
echo "*************************************"
export STORAGE_TYPE=s3
export STORAGE_S3_BUCKET=testbucket
export STORAGE_S3_ROOT=admin
export STORAGE_S3_ENDPOINT_URL=http://127.0.0.1:9900
export STORAGE_S3_ACCESS_KEY_ID=minioadmin
export STORAGE_S3_SECRET_ACCESS_KEY=minioadmin
export STORAGE_ALLOW_INSECURE=true

export SPILL_SPILL_LOCAL_DISK_PATH=''
export QUERY_CONFIG_DIR="./.databend/sqllogic-cluster-configs"
rm -rf "${QUERY_CONFIG_DIR}"
mkdir -p "${QUERY_CONFIG_DIR}"

for node in 1 2 3 4 5 6 7; do
	cp "scripts/ci/deploy/config/databend-query-node-${node}.toml" "${QUERY_CONFIG_DIR}/"
	cat >>"${QUERY_CONFIG_DIR}/databend-query-node-${node}.toml" <<'EOF'

[spill.storage]
type = "s3"

[spill.storage.s3]
bucket = "spillbucket"
root = "admin"
endpoint_url = "http://127.0.0.1:9900"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
allow_insecure = true
EOF
done

echo "Starting Cluster databend-query"
./scripts/ci/deploy/databend-query-cluster-7-nodes.sh

export RUST_BACKTRACE=1

TEST_HANDLERS=${TEST_HANDLERS:-"mysql,http"}
TEST_PARALLEL=${TEST_PARALLEL:-8}
TEST_REPEAT=${TEST_REPEAT:-1}
BUILD_PROFILE=${BUILD_PROFILE:-debug}

RUN_DIR=""
if [ $# -gt 0 ]; then
	RUN_DIR="--run_dir $*"
fi
echo "Run suites using argument: $RUN_DIR"

echo "Starting databend-sqllogic tests, repeat: ${TEST_REPEAT}"
for round in $(seq 1 "${TEST_REPEAT}"); do
	echo "Starting databend-sqllogic tests round ${round}/${TEST_REPEAT}"
	target/${BUILD_PROFILE}/databend-sqllogictests --handlers ${TEST_HANDLERS} ${RUN_DIR} --enable_sandbox --parallel ${TEST_PARALLEL} ${TEST_EXT_ARGS}
done
