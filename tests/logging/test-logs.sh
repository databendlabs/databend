#!/bin/bash

set -e

BUILD_PROFILE="${BUILD_PROFILE:-debug}"
SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../tests" || exit

curl --proto '=https' --tlsv1.2 -sSfL https://sh.vector.dev | sudo bash -s -- -y --prefix /usr/local

echo "Starting standalone Databend Query with logging"
./scripts/ci/deploy/databend-query-standalone-logging.sh
