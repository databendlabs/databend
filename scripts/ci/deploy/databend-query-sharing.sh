#!/bin/bash
# Copyright 2022 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.
set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../.." || exit
BUILD_PROFILE=${BUILD_PROFILE:-debug}

echo "*************************************"
echo "* Test on Databend openSharing endpoint    *"
echo "* it will start a node from another tenant *"
echo "* Please make sure that S3 backend         *"
echo "* is ready, and configured properly.       *"
echo "*************************************"
echo "Start open-sharing..."
export TENANT=test_tenant
nohup target/${BUILD_PROFILE}/open-sharing &
python3 scripts/ci/wait_tcp.py --timeout 5 --port 33003

echo 'Start databend-query...'
export STORAGE_S3_ROOT=shared

nohup target/${BUILD_PROFILE}/databend-query -c scripts/ci/deploy/config/databend-query-node-shared.toml &

python3 scripts/ci/wait_tcp.py --timeout 5 --port 53307
