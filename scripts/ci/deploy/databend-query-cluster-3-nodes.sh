#!/bin/bash
# Copyright 2022 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../.." || exit

# Caveat: has to kill query first.
# `query` tries to remove its liveness record from meta before shutting down.
# If meta is stopped, `query` will receive an error that hangs graceful
# shutdown.
killall databend-query
sleep 3

killall databend-meta
sleep 3

for bin in databend-query databend-meta; do
	if test -n "$(pgrep $bin)"; then
		echo "The $bin is not killed. force killing."
		killall -9 $bin
	fi
done

echo 'Start Meta service HA cluster(3 nodes)...'

nohup ./target/debug/databend-meta -c scripts/ci/deploy/config/databend-meta-node-1.toml &
python3 scripts/ci/wait_tcp.py --timeout 5 --port 9191

nohup ./target/debug/databend-meta -c scripts/ci/deploy/config/databend-meta-node-2.toml &
python3 scripts/ci/wait_tcp.py --timeout 5 --port 28202

nohup ./target/debug/databend-meta -c scripts/ci/deploy/config/databend-meta-node-3.toml &
python3 scripts/ci/wait_tcp.py --timeout 5 --port 28302

echo 'Start databend-query node-1'
nohup target/debug/databend-query -c scripts/ci/deploy/config/databend-query-node-1.toml &

echo "Waiting on node-1..."
python3 scripts/ci/wait_tcp.py --timeout 5 --port 9091

echo 'Start databend-query node-2'
nohup target/debug/databend-query -c scripts/ci/deploy/config/databend-query-node-2.toml &

echo "Waiting on node-2..."
python3 scripts/ci/wait_tcp.py --timeout 5 --port 9092

echo 'Start databend-query node-3'
nohup target/debug/databend-query -c scripts/ci/deploy/config/databend-query-node-3.toml &

echo "Waiting on node-3..."
python3 scripts/ci/wait_tcp.py --timeout 5 --port 9093

echo "All done..."
