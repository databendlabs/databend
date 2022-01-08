#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

echo "Starting Cluster databend-query"
./scripts/ci/deploy/databend-query-cluster-3-nodes.sh

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../tests" || exit

echo "Starting databend-test"
./databend-test '^0[^4]_' --mode 'cluster' --run-dir 0_stateless
