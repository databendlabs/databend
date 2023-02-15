#!/bin/bash

set -e

BENCHMARK_ID=${BENCHMARK_ID:-$(date +%s)}
BENCHMARK_STORAGE=${BENCHMARK_STORAGE:-fs}

echo "Checking script dependencies..."
# OpenBSD netcat do not have a version arg
# nc --version
bc --version
jq --version
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

case $BENCHMARK_STORAGE in
fs)
    nohup databend-query \
        --meta-endpoints "127.0.0.1:9191" \
        --storage-type fs \
        --storage-fs-data-path "benchmark/data/${BENCHMARK_ID}" \
        --tenant-id benchmark \
        --cluster-id "${BENCHMARK_ID}" \
        --storage-allow-insecure &
    ;;
s3)
    nohup databend-query \
        --meta-endpoints "127.0.0.1:9191" \
        --storage-type s3 \
        --storage-s3-region us-east-2 \
        --storage-s3-bucket databend-ci \
        --storage-s3-root "benchmark/data/${BENCHMARK_ID}" \
        --tenant-id benchmark \
        --cluster-id "${BENCHMARK_ID}" \
        --storage-allow-insecure &
    ;;
*)
    echo "Unknown storage type: $BENCHMARK_STORAGE"
    exit 1
    ;;
esac

echo "Waiting on databend-query 10 seconds..."
wait_for_port 8000 10

# Connect to databend-query
bendsql connect

# Load the data
case $BENCHMARK_STORAGE in
fs)
    echo "Creating table for hits with native storage format..."
    bendsql query <create_local.sql
    ;;
s3)
    echo "Creating table for hits..."
    bendsql query <create.sql
    ;;
*)
    echo "Unknown storage type: $BENCHMARK_STORAGE"
    exit 1
    ;;
esac

echo "Loading data..."
load_start=$(date +%s)
bendsql query <load.sql
load_end=$(date +%s)
load_time=$(echo "$load_end - $load_start" | bc -l)
echo "Data loaded in ${load_time}s."

data_size=$(echo "select bytes_compressed from fuse_snapshot('default' ,'hits');" | bendsql query -f unaligned -t)

echo '{}' >result.json
jq ".load_time = ${load_time} | .data_size = ${data_size} | .result = []" <result.json >result.json.tmp && mv result.json.tmp result.json

echo "Running queries..."
./run.sh
