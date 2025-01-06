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
config="[spill.storage]
type = \"s3\"

[spill.storage.s3]
bucket = \"spillbucket\"
root = \"admin\"
endpoint_url = \"http://127.0.0.1:9900\"
access_key_id = \"minioadmin\"
secret_access_key = \"minioadmin\"
allow_insecure = true"

echo "$config" >> ./scripts/ci/deploy/config/databend-query-node-1.toml

echo "Install dependence"
python3 -m pip install --quiet mysql-connector-python requests

echo "calling test suite"
echo "Starting standalone DatabendQuery(debug)"
./scripts/ci/deploy/databend-query-standalone.sh

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../tests" || exit

echo "Starting databend-test"
./databend-test $1 --mode 'standalone' --run-dir 1_stateful
