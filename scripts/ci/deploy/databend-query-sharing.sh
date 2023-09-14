#!/bin/bash
# Copyright 2023 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.
set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../.." || exit
BUILD_PROFILE=${BUILD_PROFILE:-debug}

killall databend-query || true
killall databend-meta || true
killall open-sharing || true
sleep 1

echo 'Start query node1 databend-meta...'
nohup target/${BUILD_PROFILE}/databend-meta -c scripts/ci/deploy/config/databend-meta-node-share-1.toml &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9191

echo "Start query node1 for sharding data"
nohup target/${BUILD_PROFILE}/databend-query -c scripts/ci/deploy/config/databend-query-node-share-1.toml &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 13307

echo 'Start query node2 databend-meta...'
nohup target/${BUILD_PROFILE}/databend-meta -c scripts/ci/deploy/config/databend-meta-node-share-2.toml &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 19191

echo 'Start query node2 open-sharing...'
nohup target/${BUILD_PROFILE}/open-sharing --tenant=shared_tenant --storage-type=s3 --storage-s3-bucket=testbucket --storage-s3-endpoint-url=http://127.0.0.1:9900 --storage-s3-access-key-id=minioadmin --storage-s3-secret-access-key=minioadmin --storage-allow-insecure --share-endpoint-address=127.0.0.1:23003 &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 23003

echo "Start query node2 for sharding data"
nohup target/${BUILD_PROFILE}/databend-query -c scripts/ci/deploy/config/databend-query-node-share-2.toml &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 13307

echo 'Start query node3 databend-meta...'
nohup target/${BUILD_PROFILE}/databend-meta -c scripts/ci/deploy/config/databend-meta-node-share-3.toml &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 39191

echo 'Start query node3 open-sharing...'
nohup target/${BUILD_PROFILE}/open-sharing --tenant=shared_tenant --storage-type=s3 --storage-s3-bucket=testbucket --storage-s3-endpoint-url=http://127.0.0.1:9900 --storage-s3-access-key-id=minioadmin --storage-s3-secret-access-key=minioadmin --storage-allow-insecure --share-endpoint-address=127.0.0.1:23103 &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 23103

echo "Start query node3 with the same tenant and meta server for sharding data"
nohup target/${BUILD_PROFILE}/databend-query -c scripts/ci/deploy/config/databend-query-node-share-3.toml &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 13317
