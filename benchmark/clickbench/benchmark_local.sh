#!/bin/bash

set -e

BENCHMARK_ID=${BENCHMARK_ID:-$(date +%s)}
BENCHMARK_DATASET=${BENCHMARK_DATASET:-hits}

echo "###############################################"
echo "Running benchmark for databend local storage..."

echo "Checking script dependencies..."
# OpenBSD netcat do not have a version arg
# nc --version
bc --version
yq --version
bendsql version

function wait_for_port() {
    # Wait for a port to be open
    # Usage: wait_for_port 8080 10
    # Args:
    #     $1: port
    #     $2: timeout in seconds
    local port=$1
    local timeout=$2
    local start_time
    start_time=$(date +%s)
    local end_time
    end_time=$((start_time + timeout))
    while true; do
        if nc -z localhost "$port"; then
            echo "OK: ${port} is listening"
            return 0
        fi
        if [[ $(date +%s) -gt $end_time ]]; then
            echo "Wait for port ${port} time out!"
            return 2
        fi
        echo "Waiting for port ${port} up..."
        sleep 1
    done
}

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
nohup databend-meta --single &
echo "Waiting on databend-meta 10 seconds..."
wait_for_port 9191 10
echo 'Start databend-query...'

nohup databend-query \
    --meta-endpoints "127.0.0.1:9191" \
    --storage-type fs \
    --storage-fs-data-path "benchmark/data/${BENCHMARK_ID}/${BENCHMARK_DATASET}/" \
    --tenant-id benchmark \
    --cluster-id "${BENCHMARK_ID}" \
    --storage-allow-insecure &

echo "Waiting on databend-query 10 seconds..."
wait_for_port 8000 10

# Connect to databend-query
bendsql connect --database "${BENCHMARK_DATASET}"
echo "CREATE DATABASE ${BENCHMARK_DATASET};" | bendsql query

# Load the data
echo "Creating table for benchmark with native storage format..."
bendsql query <"${BENCHMARK_DATASET}/create_local.sql"

echo "Loading data..."
load_start=$(date +%s)
bendsql query <"${BENCHMARK_DATASET}/load.sql"
load_end=$(date +%s)
load_time=$(echo "$load_end - $load_start" | bc -l)
echo "Data loaded in ${load_time}s."

data_size=$(echo "select sum(data_compressed_size) from system.tables where database = '${BENCHMARK_DATASET}';" | bendsql query -f unaligned -t)

echo '{}' >result.json
yq -i ".load_time = ${load_time} | .data_size = ${data_size} | .result = []" result.json

echo "Running queries..."

function run_query() {
    local query_num=$1
    local seq=$2
    local query=$3

    local q_start q_end q_time

    q_start=$(date +%s.%N)
    if echo "$query" | bendsql query --format csv --rows-only >/dev/null; then
        q_end=$(date +%s.%N)
        q_time=$(echo "$q_end - $q_start" | bc -l)
        echo "Q${query_num}[$seq] succeeded in $q_time seconds"
        yq -i ".result[${query_num}] += [${q_time}]" result.json
    else
        echo "Q${query_num}[$seq] failed"
    fi
}

TRIES=5
QUERY_NUM=0
while read -r query; do
    echo "Running Q${QUERY_NUM}: ${query}"
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches
    yq -i ".result += [[]]" result.json
    for i in $(seq 1 $TRIES); do
        run_query "$QUERY_NUM" "$i" "$query"
    done
    QUERY_NUM=$((QUERY_NUM + 1))
done <"${BENCHMARK_DATASET}/queries.sql"
