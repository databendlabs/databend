#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

source ./scripts/ci/ci-run-sqllogic-common.sh

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
config="[spill.storage]
type = \"s3\"

[spill.storage.s3]
bucket = \"spillbucket\"
root = \"admin\"
endpoint_url = \"http://127.0.0.1:9900\"
access_key_id = \"minioadmin\"
secret_access_key = \"minioadmin\"
allow_insecure = true"

echo "$config" >>./scripts/ci/deploy/config/databend-query-node-1.toml

echo "Starting standalone DatabendQuery and DatabendMeta"
./scripts/ci/deploy/databend-query-standalone.sh

TEST_HANDLERS=${TEST_HANDLERS:-"mysql,http"}
TEST_PARALLEL=${TEST_PARALLEL:-8}
BUILD_PROFILE=${BUILD_PROFILE:-debug}


echo "Starting databend-sqllogic tests"
if [ -n "${1:-}" ]; then
	sqllogic_filter "$1"
else
	sqllogic_filter skip-management-all
fi
target/${BUILD_PROFILE}/databend-sqllogictests "${SQLLOGIC_FILTER[@]}" --handlers ${TEST_HANDLERS} --enable_sandbox --parallel ${TEST_PARALLEL}
