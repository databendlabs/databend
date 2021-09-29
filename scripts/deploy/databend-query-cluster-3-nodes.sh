#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../.." || exit

killall databend-query
killall databend-kvsvr
sleep 1

echo 'Start one databend-kvsvr...'
nohup target/debug/databend-kv  --log-level=ERROR &
echo "Waiting on databend-kv 10 seconds..."
python scripts/ci/wait_tcp.py --timeout 5 --port 28003

echo 'Start DatabendQuery node-1'
nohup target/debug/databend-query -c scripts/deploy/config/databend-query-node-1.toml &

echo "Waiting on node-1..."
python scripts/ci/wait_tcp.py --timeout 5 --port 9091

echo 'Start DatabendQuery node-2'
nohup target/debug/databend-query -c scripts/deploy/config/databend-query-node-2.toml &

echo "Waiting on node-2..."
python scripts/ci/wait_tcp.py --timeout 5 --port 9092

echo 'Start DatabendQuery node-3'
nohup target/debug/databend-query -c scripts/deploy/config/databend-query-node-3.toml &

echo "Waiting on node-3..."
python scripts/ci/wait_tcp.py --timeout 5 --port 9093

echo "All done..."
