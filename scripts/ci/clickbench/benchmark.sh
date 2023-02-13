#!/bin/bash

set -e

BENCHMARK_ID=${BENCHMARK_ID:-$(date +%s)}

killall databend-query || true
killall databend-meta || true
sleep 1
for bin in databend-query databend-meta; do
    if test -n "$(pgrep $bin)"; then
        echo "The $bin is not killed. force killing."
        killall -9 $bin || true
    fi
done
echo 'Start databend-meta...'
nohup target/release/databend-meta --single &
echo "Waiting on databend-meta 10 seconds..."
python3 scripts/ci/wait_tcp.py --timeout 5 --port 9191
echo 'Start databend-query...'
nohup target/release/databend-query \
    --meta-endpoints 127.0.0.1:9191 \
    --storage-type s3 \
    --storage-s3-region us-east-2 \
    --storage-s3-bucket databend-ci \
    --storage-s3-root "perf/data/${BENCHMARK_ID}" \
    --tenant-id perf \
    --cluster-id "${BENCHMARK_ID}" \
    --storage-allow-insecure &
echo "Waiting on databend-query 10 seconds..."
python3 scripts/ci/wait_tcp.py --timeout 5 --port 3307

# Connect to databend-query
bendsql connect

# Load the data
bendsql query <create.sql

load_start=$(date +%s)
bendsql query <load_from_repo.sql
load_end=$(date +%s)
load_time=$(echo "$load_end - $load_start" | bc -l)

jq ".load_time = ${load_time}" <result.json >result.json.tmp && mv result.json.tmp result.json

echo 'select count(*) from hits;' | bendsql query

jq '.result = []' <result.json >result.json.tmp && mv result.json.tmp result.json
./run.sh
