#!/bin/bash
## use this script to test merge into performance

set -e

BENCHMARK_ID=${BENCHMARK_ID:-$(date +%s)}
BENCHMARK_DATASET=${BENCHMARK_DATASET:-merge_into}

echo "###############################################"
echo "Running benchmark for databend local storage..."

echo "Checking script dependencies..."
python3 --version
yq --version
bendsql --version

killall databend-query || true
killall databend-meta || true
sleep 1
for bin in databend-query databend-meta; do
    if test -n "$(pgrep $bin)"; then
        echo "The $bin is not killed. force killing."
        killall -9 $bin || true
    fi
done

# Wait for killed process to cleanup resources
sleep 1

echo 'Start databend-meta...'
nohup ./databend-meta --single &
echo "Waiting on databend-meta 10 seconds..."
./wait_tcp.py --port 9191 --timeout 10
echo 'Start databend-query...'

cat <<EOF >config.toml
[query]
tenant_id = "benchmark"
cluster_id = "${BENCHMARK_ID}"
[[query.users]]
name = "root"
auth_type = "no_password"
[meta]
endpoints = ["127.0.0.1:9191"]
[storage]
type = "fs"
[storage.fs]
data_path = "benchmark/data/${BENCHMARK_ID}/${BENCHMARK_DATASET}/"
EOF

nohup ./databend-query --config-file config.toml --storage-allow-insecure &

echo "Waiting on databend-query 10 seconds..."
./wait_tcp.py --port 8000 --timeout 10

# Connect to databend-query

export BENDSQL_DSN="databend://root:@localhost:8000/${BENCHMARK_DATASET}?sslmode=disable"
echo "CREATE DATABASE ${BENCHMARK_DATASET};" | bendsql

# Load the data
echo "Creating table for benchmark with native storage format..."
bendsql <"${BENCHMARK_DATASET}/create_local.sql"

# Detect instance type
instance_type=$(uname -a)
echo "Instance type: ${instance_type}"
echo "SystemInfo: $(system_profiler SPHardwareDataType)"
echo "Loading data..."
load_start=$(date +%s)
bendsql <"${BENCHMARK_DATASET}/load.sql"
load_end=$(date +%s)
load_time=$(python3 -c "print($load_end - $load_start)")
echo "Data loaded in ${load_time}s."

data_size=$(echo "select sum(data_compressed_size) from system.tables where database = '${BENCHMARK_DATASET}';" | bendsql -o tsv)
format_instance_type=$(fold -w 40 <<<"$instance_type")
echo '{}' >result.json
yq -i ".date = \"$(date -u +%Y-%m-%d)\"" -o json result.json
yq -i ".load_time = ${load_time} | .data_size = ${data_size} | .result = []" -o json result.json
yq -i ".machine = \"${format_instance_type}\"" -o json result.json
yq -i '.cluster_size = 1' -o json result.json
yq -i '.tags = ["gp3"]' -o json result.json
yq -i ".system = \"${1}\"" -o json result.json

echo "Running queries..."

function run_query() {
    local query_num=$1
    local seq=$2
    local query=$3

    local q_time
    q_time=$(echo "$query" | bendsql --time=server)
    if [[ -n $q_time ]]; then
        echo "Q${query_num}[$seq] succeeded in $q_time seconds"
        yq -i ".result[${query_num}] += [${q_time}]" -o json result.json
    else
        echo "Q${query_num}[$seq] failed"
    fi
}

TRIES=1
QUERY_NUM=0
while read -r query; do
    echo "Running Q${QUERY_NUM}: ${query}"
    sync
    yq -i ".result += [[]]" -o json result.json
    for i in $(seq 1 $TRIES); do
        run_query "$QUERY_NUM" "$i" "$query"
    done
    QUERY_NUM=$((QUERY_NUM + 1))
done <"${BENCHMARK_DATASET}/queries.sql"
