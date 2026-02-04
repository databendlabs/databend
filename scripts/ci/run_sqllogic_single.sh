#!/usr/bin/env bash
# Run a single sqllogictest file in standalone mode (CI-like).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." >/dev/null 2>&1 && pwd)"

TEST_FILE="r_cte_union_all.test"
HANDLERS="${HANDLERS:-mysql,http}"
PARALLEL="${PARALLEL:-16}"
BUILD_PROFILE="${BUILD_PROFILE:-debug}"
RUNS="${RUNS:-100}"

cd "${ROOT_DIR}"

dump_logs() {
  echo "=== debug: ps databend ==="
  ps -ef | grep -E "databend-(meta|query)" | grep -v grep || true

  echo "=== debug: nohup.out (tail) ==="
  tail -n 200 nohup.out || true

  echo "=== debug: query logs (tail) ==="
  if ls .databend/logs_1/databend-query-* >/dev/null 2>&1; then
    tail -n 200 .databend/logs_1/databend-query-* || true
  else
    echo "no query log files"
  fi

  echo "=== debug: meta logs (tail) ==="
  if ls .databend/logs1/databend-meta-* >/dev/null 2>&1; then
    tail -n 200 .databend/logs1/databend-meta-* || true
  else
    echo "no meta log files"
  fi
}

on_exit() {
  status=$?
  if [ "${status}" -ne 0 ]; then
    echo "run failed with status ${status}"
    dump_logs
  fi
}
trap on_exit EXIT

echo "Start standalone databend-meta/databend-query..."
killall databend-query databend-meta >/dev/null 2>&1 || true
sleep 1

for bin in databend-query databend-meta; do
  if test -n "$(pgrep "$bin")"; then
    echo "The $bin is not killed. force killing."
    killall -9 "$bin" || true
  fi
done

sleep 1

nohup "target/${BUILD_PROFILE}/databend-meta" \
  -c "scripts/ci/deploy/config/databend-meta-node-1.toml" &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 9191

nohup "target/${BUILD_PROFILE}/databend-query" \
  -c "scripts/ci/deploy/config/databend-query-node-1.toml" \
  --meta-endpoints "127.0.0.1:9191" \
  --internal-enable-sandbox-tenant &
python3 scripts/ci/wait_tcp.py --timeout 30 --port 8000

echo "Run sqllogictests"
echo "  file: ${TEST_FILE}"
echo "  handlers: ${HANDLERS}"
echo "  parallel: ${PARALLEL}"
echo "  build: ${BUILD_PROFILE}"
echo "  runs: ${RUNS}"

for i in $(seq 1 "${RUNS}"); do
  echo "=== run ${i} ==="
  "target/${BUILD_PROFILE}/databend-sqllogictests" \
    --handlers "${HANDLERS}" \
    --run_file "${TEST_FILE}" \
    --enable_sandbox \
    --parallel "${PARALLEL}"
done

echo "1"