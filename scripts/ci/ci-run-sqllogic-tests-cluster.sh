#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

export STORAGE_ALLOW_INSECURE=true

echo "Starting Cluster databend-query"
./scripts/ci/deploy/databend-query-cluster-3-nodes.sh

export RUST_BACKTRACE=1

TEST_HANDLERS=${TEST_HANDLERS:-"mysql,http"}
TEST_PARALLEL=${TEST_PARALLEL:-8}
BUILD_PROFILE=${BUILD_PROFILE:-debug}

RUN_ARGS=()
EXTRA_ARGS=()
QUERY_LOG_DIRS=(./.databend/logs_1/query-details)
if [ $# -gt 0 ]; then
	if [ "$#" -eq 1 ] && [ "$1" = "concurrent" ]; then
		if ! [[ "$TEST_PARALLEL" =~ ^[1-9][0-9]*$ ]]; then
			echo "TEST_PARALLEL must be a positive integer for concurrent, got: ${TEST_PARALLEL}"
			exit 1
		fi

		source_file="tests/sqllogictests/suites/concurrent/queries.test"
		generated_dir="tests/sqllogictests/suites/concurrent/generated"
		rm -rf "${generated_dir}"
		mkdir -p "${generated_dir}"
		for i in $(seq 1 "${TEST_PARALLEL}"); do
			printf -v generated_file "%s/queries_%03d.test" "${generated_dir}" "${i}"
			cp "${source_file}" "${generated_file}"
		done
		RUN_ARGS=(--run "${generated_dir}/*.test")
		EXTRA_ARGS=(--http-ports "${CONCURRENT_PORTS:-8000,8002,8003}")
		QUERY_LOG_DIRS=(./.databend/logs_1/query-details ./.databend/logs_2/query-details ./.databend/logs_3/query-details)
	else
		RUN_ARGS=(--run_dir "$*")
	fi
fi
echo "Run suites using argument: ${RUN_ARGS[*]}"

echo "Starting databend-sqllogic tests"
target/${BUILD_PROFILE}/databend-sqllogictests --handlers "${TEST_HANDLERS}" "${RUN_ARGS[@]}" --enable_sandbox --parallel "${TEST_PARALLEL}" "${EXTRA_ARGS[@]}" ${TEST_EXT_ARGS}

echo "Checking query logs for duplicate query_id entries"
for log_dir in "${QUERY_LOG_DIRS[@]}"; do
	python3 scripts/ci/ci-check-query-log-duplicates.py "${log_dir}"
done
