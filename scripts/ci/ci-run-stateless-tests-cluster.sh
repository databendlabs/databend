#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

echo "Starting Cluster databend-query"

# Enable backtrace to debug https://github.com/datafuselabs/databend/issues/7986
export RUST_BACKTRACE=full
./scripts/ci/deploy/databend-query-cluster-3-nodes.sh

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../tests" || exit

echo "Starting databend-test"
# 13_0004_q4: https://github.com/datafuselabs/databend/issues/8107
./databend-test --mode 'cluster' --run-dir 0_stateless --skip '13_0004_q4'
