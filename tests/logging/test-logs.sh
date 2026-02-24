#!/bin/bash

set -e

BUILD_PROFILE="${BUILD_PROFILE:-debug}"
SCRIPT_PATH="$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd)"
cd "$SCRIPT_PATH/../../" || exit

if vector -V; then
  echo "vector is already installed"
else
  echo "Installing vector"
  curl --proto '=https' --tlsv1.2 -sSfL https://sh.vector.dev | sudo bash -s -- -y --prefix /usr/local
fi

echo "Starting standalone Databend Query with logging"
./scripts/ci/deploy/databend-query-standalone-logging.sh

echo "Clean previous logs"
rm -rf .databend/vector/*

echo "Starting Vector"
killall vector || true
sleep 1
nohup vector --config-yaml ./tests/logging/vector/config.yaml &
python3 scripts/ci/wait_tcp.py --timeout 10 --port 4317

NOW1=$(date +%s)
echo $NOW1
echo "SELECT ${NOW1}" | bendsql >> result.res

sleep 3

NOW2=$(date +%s)
echo $NOW2
echo "EXPLAIN ANALYZE SELECT ${NOW2}" | bendsql >> result.res

sleep 3

NOW3=$(( $(date +%s) + 100 ))
echo $NOW3
RECURSIVE_SQL="WITH RECURSIVE t(n) AS (SELECT ${NOW3} UNION ALL SELECT n + 1 FROM t WHERE n < ${NOW3} + 3) SELECT * FROM t"
echo "${RECURSIVE_SQL}" | bendsql >> result.res

sleep 3

NOW4=$(date +%s)
echo $NOW4
echo "EXPLAIN PERF SELECT ${NOW4}" | bendsql >> result.res

sleep 1
echo "Exiting..."
killall databend-query || true
killall vector || true
sleep 1

cat result.res

./tests/logging/check_logs.py --sql "SELECT ${NOW1}" --profile-keyword "${NOW1}"
./tests/logging/check_logs.py --sql "EXPLAIN ANALYZE SELECT ${NOW2}" --profile-keyword "${NOW2}"
./tests/logging/check_logs.py --sql "${RECURSIVE_SQL}" --profile-keyword "${NOW3}"
./tests/logging/check_logs.py --sql "EXPLAIN PERF SELECT ${NOW4}" --profile-keyword "${NOW4}"
