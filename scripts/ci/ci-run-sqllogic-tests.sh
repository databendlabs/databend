#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

export STORAGE_ALLOW_INSECURE=true

echo "Starting standalone DatabendQuery and DatabendMeta"
./scripts/ci/deploy/databend-query-standalone.sh

TEST_HANDLERS=${TEST_HANDLERS:-"mysql,http"}
TEST_PARALLEL=${TEST_PARALLEL:8}
BUILD_PROFILE=${BUILD_PROFILE:-debug}

RUN_DIR=""
if [ $# -gt 0 ]; then
	RUN_DIR="--run_dir $*"
fi
echo "Run suites using argument: $RUN_DIR"

echo "Starting databend-sqllogic tests"
if [ -z "$RUN_DIR" ]; then
	target/${BUILD_PROFILE}/databend-sqllogictests --run_dir temp_table --enable_sandbox --parallel ${TEST_PARALLEL}
fi
target/${BUILD_PROFILE}/databend-sqllogictests --handlers ${TEST_HANDLERS} ${RUN_DIR} --skip_dir management,explain_native,ee,temp_table --enable_sandbox --parallel ${TEST_PARALLEL}
