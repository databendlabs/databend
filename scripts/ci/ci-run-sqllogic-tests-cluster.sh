#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.
set -e

echo "Starting Cluster databend-query"
./scripts/ci/deploy/databend-query-cluster-3-nodes.sh

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../tests/logictest" || exit

RUN_DIR=""
if [ $# -gt 0 ]; then
	RUN_DIR="--run-dir $*"
fi
echo "Run suites using argument: $RUN_DIR"

echo "pip list"
python3 -m pip list

echo "Starting databend-sqllogic tests"
python3 main.py $RUN_DIR

echo "Starting databend-sqllogic mode cluster"
python3 main.py --suite suites/mode --run-dir cluster
