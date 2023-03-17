#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

TEST_ID=${TEST_ID:-$(date +%s)}

echo "*************************************"
echo "* Setting STORAGE_TYPE to S3.       *"
echo "*                                   *"
echo "* Please make sure that S3 backend  *"
echo "* is ready, and configured properly.*"
echo "*************************************"
export STORAGE_TYPE=s3
export STORAGE_S3_BUCKET=databend-ci
export STORAGE_S3_REGION=us-east-2
export STORAGE_S3_ROOT="stateful/${TEST_ID}"
export STORAGE_ALLOW_INSECURE=true

echo "Install dependencies"
python3 -m pip install --quiet mysql-connector-python

echo "calling test suite"
echo "Starting standalone DatabendQuery(debug)"
./scripts/ci/deploy/databend-query-standalone.sh

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../tests" || exit

echo "Starting databend-test"
./databend-test "$1" --mode 'standalone' --run-dir 4_stateful_large_data
