#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

echo "Starting standalone DatabendQuery and DatabendMeta"
./scripts/ci/deploy/databend-query-standalone.sh

echo "Install rust"
./scripts/setup/dev_setup.sh

RUN_DIR=""
if [ $# -gt 0 ]; then
	RUN_DIR="--run_dir $*"
fi
echo "Run dir using argument: $RUN_DIR"
echo -e "ulimit:\n$(ulimit -a)"

echo "Starting databend-sqllogic tests under mysql"
cargo run -p sqllogictests -- ${RUN_DIR} --handler mysql

echo "Starting databend-sqllogic tests under http"
cargo run -p sqllogictests -- ${RUN_DIR} --handler http

echo "Starting databend-sqllogic tests under clickhouse"
cargo run -p sqllogictests -- ${RUN_DIR} --handler clickhouse
