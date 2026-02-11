#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

export STORAGE_ALLOW_INSECURE=true

echo "Starting Cluster databend-query"
./scripts/ci/deploy/databend-query-cluster-3-nodes.sh

ASYNC_TASKS_DUMP_OUTPUT=${ASYNC_TASKS_DUMP_OUTPUT:-"./.databend/async_tasks_dump.log"}
ASYNC_TASKS_DUMP_INTERVAL=${ASYNC_TASKS_DUMP_INTERVAL:-1}
ASYNC_TASKS_DUMP_ENDPOINTS=${ASYNC_TASKS_DUMP_ENDPOINTS:-"http://127.0.0.1:8080/debug/async_tasks/dump,http://127.0.0.1:8082/debug/async_tasks/dump,http://127.0.0.1:8083/debug/async_tasks/dump"}

echo "Starting async tasks dump collector"
bash ./scripts/ci/ci-collect-async-tasks-dump.sh \
	"${ASYNC_TASKS_DUMP_OUTPUT}" \
	"${ASYNC_TASKS_DUMP_INTERVAL}" \
	"${ASYNC_TASKS_DUMP_ENDPOINTS}" &
ASYNC_TASKS_DUMP_PID=$!

cleanup_async_tasks_dump_collector() {
	if [ -n "${ASYNC_TASKS_DUMP_PID:-}" ] && kill -0 "${ASYNC_TASKS_DUMP_PID}" 2>/dev/null; then
		echo "Stopping async tasks dump collector"
		kill "${ASYNC_TASKS_DUMP_PID}" 2>/dev/null || true
		wait "${ASYNC_TASKS_DUMP_PID}" 2>/dev/null || true
	fi
}
trap cleanup_async_tasks_dump_collector EXIT

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
