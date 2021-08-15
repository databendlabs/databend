#!/bin/bash
# Copyright 2020-2021 The Datafuse Authors.
# SPDX-License-Identifier: Apache-2.0.

echo "Starting Cluster datafuse-query"
./scripts/deploy/datafuse-query-cluster-3-nodes.sh

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../tests" || exit

echo "Starting datafuse-test"
./datafuse-test '^0[^4]_' --mode 'cluster'
