#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

source ./scripts/ci/ci-run-sqllogic-common.sh

export STORAGE_ALLOW_INSECURE=true

echo "Starting Cluster databend-query"
./scripts/ci/deploy/databend-query-cluster-3-nodes.sh

export RUST_BACKTRACE=1

TEST_HANDLERS=${TEST_HANDLERS:-"mysql,http"}
TEST_PARALLEL=${TEST_PARALLEL:-8}
BUILD_PROFILE=${BUILD_PROFILE:-debug}


echo "Starting databend-sqllogic tests"
if [ -n "${1:-}" ]; then
	sqllogic_filter "$1"
else
	sqllogic_filter all
fi
target/${BUILD_PROFILE}/databend-sqllogictests "${SQLLOGIC_FILTER[@]}" --handlers ${TEST_HANDLERS} --enable_sandbox --parallel ${TEST_PARALLEL} ${TEST_EXT_ARGS}


echo "Checking query logs for duplicate query_id entries"
python3 scripts/ci/ci-check-query-log-duplicates.py .databend/logs_1/query-details
