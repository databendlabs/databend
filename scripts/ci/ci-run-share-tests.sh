#!/bin/bash
# Copyright 2020-2023 The Databend Authors.
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
export STORAGE_S3_ROOT=
export STORAGE_S3_ENDPOINT_URL=http://127.0.0.1:9900
export STORAGE_S3_ACCESS_KEY_ID=minioadmin
export STORAGE_S3_SECRET_ACCESS_KEY=minioadmin
export STORAGE_ALLOW_INSECURE=true

echo "Install dependence"
python3 -m pip install --quiet mysql-connector-python
SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"

./scripts/ci/deploy/databend-query-share.sh

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../tests" || exit

#./databend-test $1 --mode 'standalone' --run-dir 3_stateful_sharing --print-time --timeout 120
