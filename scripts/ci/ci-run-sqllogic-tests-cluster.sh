#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

usage()
{
    cat <<-END
	$0 [--parallel <n=8>] [<dir1> <dir2>]
	Run sqllogictests with <n> threads in parallel.
	If <dir>s are provided, only run tests in these dir.
	END

    exit 0
}

export STORAGE_ALLOW_INSECURE=true

echo "Starting Cluster databend-query"
./scripts/ci/deploy/databend-query-cluster-3-nodes.sh

export RUST_BACKTRACE=1

TEST_HANDLERS=${TEST_HANDLERS:-"mysql,http,clickhouse"}
BUILD_PROFILE=${BUILD_PROFILE:-debug}

PARALLEL=8
while [ $# -gt 0 ]; do
    case "$1" in
        --parallel)
            shift
            PARALLEL="$1"
            shift
            ;;

        *)
            break
            ;;
    esac
done
echo "Run suites in parallel: $PARALLEL"

RUN_DIR=""
if [ $# -gt 0 ]; then
	RUN_DIR="--run_dir $*"
fi
echo "Run suites using argument: $RUN_DIR"

echo "Starting databend-sqllogic tests"
target/${BUILD_PROFILE}/databend-sqllogictests \
    --debug \
    --handlers ${TEST_HANDLERS} \
    ${RUN_DIR} \
    --enable_sandbox \
    --parallel "$PARALLEL" \
    --skip_file tpcds_q64.test
