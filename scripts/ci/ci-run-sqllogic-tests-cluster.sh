#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

export STORAGE_ALLOW_INSECURE=true

if [ -n "${QUERY_HTTP_HANDLER_RESULT_TIMEOUT_SECS:-}" ]; then
	echo "Use QUERY_HTTP_HANDLER_RESULT_TIMEOUT_SECS=${QUERY_HTTP_HANDLER_RESULT_TIMEOUT_SECS}"
	export QUERY_HTTP_HANDLER_RESULT_TIMEOUT_SECS
else
	unset QUERY_HTTP_HANDLER_RESULT_TIMEOUT_SECS
fi

echo "Starting Cluster databend-query"
./scripts/ci/deploy/databend-query-cluster-3-nodes.sh

export RUST_BACKTRACE=1

TEST_HANDLERS=${TEST_HANDLERS:-"mysql,http"}
TEST_PARALLEL=${TEST_PARALLEL:-8}
BUILD_PROFILE=${BUILD_PROFILE:-debug}

RUN_DIR=""
if [ $# -gt 0 ]; then
	RUN_DIR="--run_dir $*"
fi
echo "Run suites using argument: $RUN_DIR"

echo "Starting databend-sqllogic tests"
target/${BUILD_PROFILE}/databend-sqllogictests --handlers ${TEST_HANDLERS} ${RUN_DIR} --enable_sandbox --parallel ${TEST_PARALLEL} ${TEST_EXT_ARGS}
