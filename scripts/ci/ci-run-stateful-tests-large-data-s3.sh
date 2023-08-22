#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

TEST_ID=${TEST_ID:-$(date +%s)}

echo "*******************************************"
echo "* Setting STORAGE_TYPE to Object Storage. *"
echo "*                                         *"
echo "* Please make sure that storage backend   *"
echo "* is ready, and configured properly.      *"
echo "*******************************************"
case $RUNNER_PROVIDER in
"aws")
	export STORAGE_TYPE=s3
	export STORAGE_S3_BUCKET=databend-ci
	export STORAGE_S3_REGION=us-east-2
	export STORAGE_S3_ROOT="stateful/${TEST_ID}"
	export STORAGE_ALLOW_INSECURE=true
	;;
"gcp")
	export STORAGE_TYPE=gcs
	export STORAGE_GCS_BUCKET=databend-ci
	export STORAGE_GCS_REGION=us-central1
	export STORAGE_GCS_ROOT="stateful/${TEST_ID}"
	;;
*)
	echo "Unknown provider: $RUNNER_PROVIDER"
	exit 1
	;;
esac

echo "Install dependencies"
python3 -m pip install --quiet mysql-connector-python

echo "calling test suite"
echo "Starting standalone DatabendQuery(debug)"
./scripts/ci/deploy/databend-query-standalone.sh

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../tests" || exit

echo "Starting databend-test"
./databend-test "$1" --mode 'standalone' --run-dir 4_stateful_large_data
