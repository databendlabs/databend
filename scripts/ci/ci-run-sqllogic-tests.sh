#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

echo "Starting standalone DatabendQuery and DatabendMeta"
./scripts/ci/deploy/databend-query-standalone.sh

TEST_HANDLERS=${TEST_HANDLERS:-"mysql,http,clickhouse"}

RUN_DIR=""
if [ $# -gt 0 ]; then
	RUN_DIR="--run_dir $*"
fi
echo "Run suites using argument: $RUN_DIR"

echo "Starting databend-sqllogic tests"
cargo run -p sqllogictests -- --handlers ${TEST_HANDLERS} ${RUN_DIR}
