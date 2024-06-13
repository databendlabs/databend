#!/bin/bash

set -e

BUILD_PROFILE="${BUILD_PROFILE:-debug}"
SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../" || exit

if vector -V; then
  echo "vector is already installed"
else
  echo "Installing vector" curl --proto '=https' --tlsv1.2 -sSfL https://sh.vector.dev | sudo bash -s -- -y --prefix /usr/local
fi

echo "Starting standalone Databend Query with logging"
./scripts/ci/deploy/databend-query-standalone-logging.sh

echo "Clean previous logs"
rm -rf .databend/vector/*

echo "Starting Vector"
killall vector || true
sleep 1
nohup vector --config-yaml ./tests/logging/vector/config.yaml &
python3 scripts/ci/wait_tcp.py --timeout 10 --port 4317
