#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

source ./scripts/ci/ci-minio-common.sh

if has_minio_sqllogic_suite "$@"; then
	setup_minio_storage_env
	append_minio_spill_config \
		./scripts/ci/deploy/config/databend-query-node-1.toml \
		./scripts/ci/deploy/config/databend-query-node-2.toml \
		./scripts/ci/deploy/config/databend-query-node-3.toml \
		./scripts/ci/deploy/config/databend-query-node-4.toml \
		./scripts/ci/deploy/config/databend-query-node-5.toml \
		./scripts/ci/deploy/config/databend-query-node-6.toml \
		./scripts/ci/deploy/config/databend-query-node-7.toml
else
	export STORAGE_ALLOW_INSECURE=true
fi

echo "Starting Cluster databend-query"
if has_minio_sqllogic_suite "$@"; then
	./scripts/ci/deploy/databend-query-cluster-7-nodes.sh
else
	./scripts/ci/deploy/databend-query-cluster-3-nodes.sh
fi

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
