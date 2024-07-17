#!/bin/bash
# Copyright 2023 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.
set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../.." || exit
BUILD_PROFILE=${BUILD_PROFILE:-debug}

killall databend-query || true
killall databend-meta || true
killall share-endpoint || true
sleep 1

echo "current directory:"
pwd

echo "build share endpoint"
ls 
mv share-endpoint ../tmp-share-endpoint
cd ../tmp-share-endpoint
cargo build --release
cd -

echo 'Start share endpoint...'
nohup ../tmp-share-endpoint/target/release/share-endpoint -c scripts/ci/deploy/config/share-endpoint.toml &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 22322

echo 'Start query node provider databend-meta...'
nohup target/${BUILD_PROFILE}/databend-meta -c scripts/ci/deploy/config/databend-meta-node-share-provider.toml &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9191

echo "Start query provider for sharding data"
nohup target/${BUILD_PROFILE}/databend-query -c scripts/ci/deploy/config/databend-query-node-share-provider.toml &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 13307

echo 'Start query node consumer databend-meta...'
nohup target/${BUILD_PROFILE}/databend-meta -c scripts/ci/deploy/config/databend-meta-node-share-consumer.toml &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 19191

echo "Start query consumer for sharding data"
nohup target/${BUILD_PROFILE}/databend-query -c scripts/ci/deploy/config/databend-query-node-share-consumer.toml &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 23307