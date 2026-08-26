#!/usr/bin/env bash
# Copyright 2021 Datafuse Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../.." || exit

BUILD_PROFILE="${BUILD_PROFILE:-debug}"
BINARY_DIR="${BINARY_DIR:-target/${BUILD_PROFILE}}"
BENCH_DIR="${BENCH_DIR:-.databend/proxy-routing-benchmark}"

ROWS="${ROWS:-300000}"
TRACE_CARDINALITY="${TRACE_CARDINALITY:-17}"
CHAT_CARDINALITY="${CHAT_CARDINALITY:-19}"
USER_CARDINALITY="${USER_CARDINALITY:-59}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-64}"
WARMUP_ROUNDS="${WARMUP_ROUNDS:-1}"
MEASURE_ROUNDS="${MEASURE_ROUNDS:-2}"
PROXY_ROUTING_MODEL="${PROXY_ROUTING_MODEL:-statistics}"
ENABLE_PROXY_BLOOM_PRUNING="${ENABLE_PROXY_BLOOM_PRUNING:-0}"
MIN_TOP1_HIT_RATIO="${MIN_TOP1_HIT_RATIO:-}"
MIN_ACCEPTABLE_HIT_RATIO="${MIN_ACCEPTABLE_HIT_RATIO:-}"

cleanup() {
	kill "${QUERY_PID:-}" "${META_PID:-}" 2>/dev/null || true
	sleep 1
	kill -9 "${QUERY_PID:-}" "${META_PID:-}" 2>/dev/null || true
}
trap cleanup EXIT

rm -rf "$BENCH_DIR"
mkdir -p "$BENCH_DIR/meta" "$BENCH_DIR/query" "$BENCH_DIR/logs/meta" "$BENCH_DIR/logs/query"

killall databend-query databend-meta 2>/dev/null || true
sleep 1

echo "Start databend-meta from ${BINARY_DIR}..."
"${BINARY_DIR}/databend-meta" \
	-c scripts/ci/deploy/config/databend-meta-node-1.toml \
	--raft-dir "$BENCH_DIR/meta" \
	--log-dir "$BENCH_DIR/logs/meta" &
META_PID=$!
python3 scripts/ci/wait_tcp.py --timeout 60 --port 9191

echo "Start databend-query from ${BINARY_DIR}..."
"${BINARY_DIR}/databend-query" \
	-c scripts/ci/deploy/config/databend-query-node-1.toml \
	--storage-fs-data-path "$BENCH_DIR/query" \
	--log-dir "$BENCH_DIR/logs/query" \
	--internal-enable-sandbox-tenant &
QUERY_PID=$!
python3 scripts/ci/wait_tcp.py --timeout 90 --port 8000

benchmark_args=(
	--rows "$ROWS"
	--trace-cardinality "$TRACE_CARDINALITY"
	--chat-cardinality "$CHAT_CARDINALITY"
	--user-cardinality "$USER_CARDINALITY"
	--payload-bytes "$PAYLOAD_BYTES"
	--warmup-rounds "$WARMUP_ROUNDS"
	--measure-rounds "$MEASURE_ROUNDS"
	--proxy-routing-model "$PROXY_ROUTING_MODEL"
)

if [[ "$ENABLE_PROXY_BLOOM_PRUNING" == "1" ]]; then
	benchmark_args+=(--enable-proxy-bloom-pruning)
fi

if [[ -n "$MIN_TOP1_HIT_RATIO" ]]; then
	benchmark_args+=(--min-top1-hit-ratio "$MIN_TOP1_HIT_RATIO")
fi

if [[ -n "$MIN_ACCEPTABLE_HIT_RATIO" ]]; then
	benchmark_args+=(--min-acceptable-hit-ratio "$MIN_ACCEPTABLE_HIT_RATIO")
fi

scripts/benchmark/proxy_table_routing_bench.py "${benchmark_args[@]}"
