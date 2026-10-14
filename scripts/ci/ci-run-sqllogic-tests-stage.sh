#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.
#
# Run the `stage` sqllogic suite once per (dedup, size) variant inside a
# single CI job. The variants only differ in GLOBAL settings applied by the
# stage hook; the suite itself is not idempotent, so meta/query are redeployed
# with a clean data dir between variants. This keeps one job's setup cost
# (checkout, artifact download, minio) while covering all four variants.

set -e

source ./scripts/ci/ci-run-sqllogic-common.sh

export STORAGE_ALLOW_INSECURE=true

TEST_HANDLERS=${TEST_HANDLERS:-"http,hybrid"}
BUILD_PROFILE=${BUILD_PROFILE:-debug}
TEST_STAGE_STORAGE=${TEST_STAGE_STORAGE:-fs}
export TEST_STAGE_STORAGE

sqllogic_filter stage

for dedup in full_path sub_path; do
	for size in small large; do
		echo "Starting standalone DatabendQuery and DatabendMeta (clean state)"
		killall databend-query || true
		killall databend-meta || true
		sleep 1
		rm -rf ./.databend/meta1 ./.databend/stateless_test_data ./.databend/_cache
		./scripts/ci/deploy/databend-query-standalone.sh

		echo "Starting databend-sqllogic stage tests: storage=${TEST_STAGE_STORAGE} dedup=${dedup} size=${size}"
		TEST_STAGE_DEDUP=${dedup} TEST_STAGE_SIZE=${size} \
			target/"${BUILD_PROFILE}"/databend-sqllogictests "${SQLLOGIC_FILTER[@]}" --handlers "${TEST_HANDLERS}" --parallel 1
	done
done
