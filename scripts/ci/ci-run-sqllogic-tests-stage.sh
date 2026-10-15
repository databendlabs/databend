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

# Stop meta/query and wait until the processes are gone so their state
# directories can be removed safely (query allows up to 5s for shutdown).
servers_running() {
	pgrep -x databend-query >/dev/null || pgrep -x databend-meta >/dev/null
}

stop_servers() {
	killall databend-query databend-meta 2>/dev/null || true
	for _ in $(seq 1 20); do
		servers_running || return 0
		sleep 0.5
	done
	echo "servers still alive after SIGTERM; force killing"
	killall -9 databend-query databend-meta 2>/dev/null || true
	for _ in $(seq 1 10); do
		servers_running || return 0
		sleep 0.5
	done
	echo "failed to stop databend servers" >&2
	return 1
}

# Run every variant even if one fails (the former matrix used fail-fast: false),
# then report all failures at once.
failed=()

for dedup in full_path sub_path; do
	for size in small large; do
		echo "Starting standalone DatabendQuery and DatabendMeta (clean state)"
		stop_servers
		rm -rf ./.databend/meta1 ./.databend/stateless_test_data ./.databend/_cache
		./scripts/ci/deploy/databend-query-standalone.sh

		echo "Starting databend-sqllogic stage tests: storage=${TEST_STAGE_STORAGE} dedup=${dedup} size=${size}"
		if ! TEST_STAGE_DEDUP=${dedup} TEST_STAGE_SIZE=${size} \
			target/"${BUILD_PROFILE}"/databend-sqllogictests "${SQLLOGIC_FILTER[@]}" --handlers "${TEST_HANDLERS}" --parallel 1; then
			echo "::error::stage variant failed: storage=${TEST_STAGE_STORAGE} dedup=${dedup} size=${size}"
			failed+=("${dedup}/${size}")
		fi
	done
done

if ((${#failed[@]} > 0)); then
	echo "Failed stage variants: ${failed[*]}"
	exit 1
fi
