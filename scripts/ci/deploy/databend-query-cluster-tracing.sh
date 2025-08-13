#!/bin/bash
# Copyright 2022 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../.." || exit
BUILD_PROFILE=${BUILD_PROFILE:-debug}

killall databend-query || true
killall databend-meta || true
sleep 1

for bin in databend-query databend-meta; do
	if test -n "$(pgrep $bin)"; then
		echo "The $bin is not killed. force killing."
		killall -9 $bin || true
	fi
done

# Wait for killed process to cleanup resources
sleep 1

echo 'Start databend-meta...'
nohup target/${BUILD_PROFILE}/databend-meta -c scripts/ci/deploy/config/databend-meta-node-1.toml &
echo "Waiting on databend-meta 10 seconds..."
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9191

export RUST_BACKTRACE=1
export LOG_TRACING_ON=true
export LOG_TRACING_CAPTURE_LOG_LEVEL="Warn,databend_=Debug"

echo 'Start databend-query node-1'
nohup target/${BUILD_PROFILE}/databend-query -c scripts/ci/deploy/config/databend-query-node-1.toml --internal-enable-sandbox-tenant >./.databend/query-1.out 2>&1 &

echo "Waiting on node-1..."
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9091

echo 'Start databend-query node-2'
nohup target/${BUILD_PROFILE}/databend-query -c scripts/ci/deploy/config/databend-query-node-2.toml --internal-enable-sandbox-tenant >./.databend/query-2.out 2>&1 &

echo "Waiting on node-2..."
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9092

echo 'Start databend-query node-3'
nohup target/${BUILD_PROFILE}/databend-query -c scripts/ci/deploy/config/databend-query-node-3.toml --internal-enable-sandbox-tenant >./.databend/query-3.out 2>&1 &

echo "Waiting on node-3..."
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9093

echo "All done..."
