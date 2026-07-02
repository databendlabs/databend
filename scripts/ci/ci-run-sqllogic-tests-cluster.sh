#!/bin/bash
# Copyright 2020-2021 The Databend Authors.
# SPDX-License-Identifier: Apache-2.0.

set -e

export STORAGE_ALLOW_INSECURE=true

TCPDUMP_WRAPPER_PID=""
TCPDUMP_CHILD_PID=""
TCPDUMP_DIR="./.databend/tcpdump"
TCP_DIAG_DIR="./.databend/tcpdiag"

collect_concurrent_tcp_diagnostics() {
	local phase="$1"
	mkdir -p "${TCP_DIAG_DIR}"

	{
		echo "timestamp_utc=$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
		echo "phase=${phase}"
		echo "+ sysctl net.ipv4.tcp_abort_on_overflow net.core.somaxconn net.ipv4.tcp_max_syn_backlog"
		sysctl net.ipv4.tcp_abort_on_overflow net.core.somaxconn net.ipv4.tcp_max_syn_backlog
	} >"${TCP_DIAG_DIR}/sysctl-${phase}.txt" 2>&1 || true

	{
		echo "timestamp_utc=$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
		echo "phase=${phase}"
		echo "+ sudo ss -ltnpi 'sport = :9091 or sport = :9092 or sport = :9093'"
		sudo ss -ltnpi 'sport = :9091 or sport = :9092 or sport = :9093'
	} >"${TCP_DIAG_DIR}/ss-listen-${phase}.txt" 2>&1 || true

	{
		echo "timestamp_utc=$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
		echo "phase=${phase}"
		echo "+ netstat -s | egrep -i 'listen|overflow|reset|retrans'"
		netstat -s | egrep -i 'listen|overflow|reset|retrans'
	} >"${TCP_DIAG_DIR}/netstat-s-${phase}.txt" 2>&1 || true
}

stop_concurrent_tcpdump() {
	collect_concurrent_tcp_diagnostics "stop"

	if [ -n "${TCPDUMP_CHILD_PID}" ]; then
		sudo kill -INT "${TCPDUMP_CHILD_PID}" 2>/dev/null || true
	fi

	if [ -n "${TCPDUMP_WRAPPER_PID}" ]; then
		kill -INT "${TCPDUMP_WRAPPER_PID}" 2>/dev/null || true
		wait "${TCPDUMP_WRAPPER_PID}" 2>/dev/null || true
	fi

	if [ -d "${TCPDUMP_DIR}" ]; then
		sudo chown -R "$(id -u):$(id -g)" "${TCPDUMP_DIR}" 2>/dev/null || true
		find "${TCPDUMP_DIR}" -type f -ls || true
	fi
}

start_concurrent_tcpdump() {
	mkdir -p "${TCPDUMP_DIR}"
	trap stop_concurrent_tcpdump EXIT
	collect_concurrent_tcp_diagnostics "start"

	if ! command -v tcpdump >/dev/null 2>&1; then
		echo "tcpdump is not installed; installing it for concurrent diagnostics"
		if ! (sudo apt-get update -yq && sudo apt-get install -yq tcpdump); then
			echo "Failed to install tcpdump; continue without packet capture"
			return 0
		fi
	fi

	echo "Starting tcpdump for concurrent cluster diagnostics"
	# Keep only query-flight traffic and cap artifact size to about 80MB.
	# -s 128 keeps packet headers and a small payload prefix, enough for TCP RST/FIN analysis.
	sudo tcpdump -i any -nn -U -Z root -s 128 -C 20 -W 4 \
		-w "${TCPDUMP_DIR}/concurrent-flight.pcap" \
		"tcp and (port 9091 or port 9092 or port 9093)" \
		>"${TCPDUMP_DIR}/tcpdump.out" 2>"${TCPDUMP_DIR}/tcpdump.err" &
	TCPDUMP_WRAPPER_PID=$!
	sleep 1
	TCPDUMP_CHILD_PID="$(pgrep -P "${TCPDUMP_WRAPPER_PID}" tcpdump | head -n 1 || true)"
}

echo "Starting Cluster databend-query"
./scripts/ci/deploy/databend-query-cluster-3-nodes.sh

export RUST_BACKTRACE=1

TEST_HANDLERS=${TEST_HANDLERS:-"mysql,http"}
TEST_PARALLEL=${TEST_PARALLEL:-8}
BUILD_PROFILE=${BUILD_PROFILE:-debug}

RUN_ARGS=()
EXTRA_ARGS=()
QUERY_LOG_DIRS=(./.databend/logs_1/query-details)
ENABLE_CONCURRENT_TCPDUMP=false
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
		ENABLE_CONCURRENT_TCPDUMP=true
	else
		RUN_ARGS=(--run_dir "$*")
	fi
fi
echo "Run suites using argument: ${RUN_ARGS[*]}"

if [ "${ENABLE_CONCURRENT_TCPDUMP}" = true ]; then
	start_concurrent_tcpdump
fi

echo "Starting databend-sqllogic tests"
target/${BUILD_PROFILE}/databend-sqllogictests --handlers "${TEST_HANDLERS}" "${RUN_ARGS[@]}" --enable_sandbox --parallel "${TEST_PARALLEL}" "${EXTRA_ARGS[@]}" ${TEST_EXT_ARGS}

echo "Checking query logs for duplicate query_id entries"
for log_dir in "${QUERY_LOG_DIRS[@]}"; do
	python3 scripts/ci/ci-check-query-log-duplicates.py "${log_dir}"
done
