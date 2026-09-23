#!/bin/bash
# Copyright 2020-2023 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

source ./scripts/ci/ci-run-sqllogic-common.sh

export STORAGE_ALLOW_INSECURE=true

echo "Starting standalone DatabendQuery and DatabendMeta"
./scripts/ci/deploy/databend-query-standalone.sh

TEST_HANDLERS=${TEST_HANDLERS:-"mysql,http"}
TEST_PARALLEL=${TEST_PARALLEL:-8}
BUILD_PROFILE=${BUILD_PROFILE:-debug}


echo "Starting ee databend-sqllogic tests"
if [ -n "${1:-}" ]; then
	sqllogic_filter "$1"
else
	sqllogic_filter all
fi
target/${BUILD_PROFILE}/databend-sqllogictests "${SQLLOGIC_FILTER[@]}" --handlers ${TEST_HANDLERS} --enable_sandbox --parallel ${TEST_PARALLEL}
