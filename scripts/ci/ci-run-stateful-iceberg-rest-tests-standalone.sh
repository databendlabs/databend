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
export STORAGE_S3_BUCKET=icebergdata
export STORAGE_S3_ROOT=admin
export STORAGE_S3_ENDPOINT_URL=http://127.0.0.1:9000
export STORAGE_S3_ACCESS_KEY_ID=admin
export STORAGE_S3_SECRET_ACCESS_KEY=password
export STORAGE_ALLOW_INSECURE=true

echo "Iceberg Rest integration tests"
echo "Starting standalone DatabendQuery(debug profile)"
./scripts/ci/deploy/databend-query-standalone.sh

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../tests" || exit

echo "Starting databend-test"
./databend-test --mode 'standalone' --run-dir 3_stateful_iceberg
