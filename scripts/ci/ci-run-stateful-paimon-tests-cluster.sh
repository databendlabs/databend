#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

echo "Starting Cluster databend-query"

./scripts/ci/deploy/databend-query-cluster-3-nodes.sh

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../tests" || exit

echo "Starting Paimon stateful tests"
./databend-test \
	--mode 'cluster' \
	--run-dir 3_stateful_paimon \
	--print-time \
	'^02_0000_multi_node_scan\.sh$' \
	'^02_0001_cluster_write\.sh$'
