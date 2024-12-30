#!/bin/bash

set -e

BENCHMARK_ID=${BENCHMARK_ID:-$(date +%s)}
BENCHMARK_DATASET=${BENCHMARK_DATASET:-hits}

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
nohup databend-meta --single &
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
default_storage_format = 'parquet'
[meta]
endpoints = ["127.0.0.1:9191"]
client_timeout_in_second = 60
[storage]
type = "fs"
[storage.fs]
data_path = "benchmark/data/${BENCHMARK_ID}/${BENCHMARK_DATASET}/"
EOF

nohup databend-query --config-file config.toml --storage-allow-insecure &

echo "Waiting on databend-query 10 seconds..."
./wait_tcp.py --port 8000 --timeout 10

# Connect to databend-query

export BENDSQL_DSN="databend://root:@localhost:8000?sslmode=disable"
echo "CREATE DATABASE ${BENCHMARK_DATASET};" | bendsql
export BENDSQL_DSN="databend://root:@localhost:8000/${BENCHMARK_DATASET}?sslmode=disable"

# Create table
if [[ -f "${BENCHMARK_DATASET}/create_local.sql" ]]; then
    echo "Creating table for benchmark with parquet storage format..."
    bendsql <"${BENCHMARK_DATASET}/create_local.sql"
fi

# Detect instance type with AWS metadata
token=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
instance_type=$(curl -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-type)
echo "Instance type: ${instance_type}"

# Load data
echo "Loading data..."
load_start=$(date +%s)
if [[ -f "${BENCHMARK_DATASET}/load.sh" ]]; then
    bash "${BENCHMARK_DATASET}"/load.sh
else
    bendsql <"${BENCHMARK_DATASET}/load.sql"
fi
load_end=$(date +%s)
load_time=$(python3 -c "print($load_end - $load_start)")
echo "Data loaded in ${load_time}s."

data_size=$(echo "select sum(data_compressed_size) from system.tables where database = '${BENCHMARK_DATASET}';" | bendsql -o tsv)

echo '{}' >result.json
yq -i ".date = \"$(date -u +%Y-%m-%d)\"" -o json result.json
yq -i ".load_time = ${load_time} | .data_size = ${data_size} | .result = []" -o json result.json
yq -i ".machine = \"${instance_type}\"" -o json result.json
yq -i '.cluster_size = 1' -o json result.json
yq -i '.tags = ["gp3"]' -o json result.json

echo "Running queries..."

function run_query() {
    local query_num=$1
    local seq=$2
    local query=$3

    local q_time
    q_time=$(bendsql --time=server <"$query")
    if [[ -n $q_time ]]; then
        echo "Q${query_num}[$seq] succeeded in $q_time seconds"
        yq -i ".result[${query_num}] += [${q_time}]" -o json result.json
    else
        echo "Q${query_num}[$seq] failed"
    fi
}

TRIES=3
QUERY_NUM=0
for query in "${BENCHMARK_DATASET}"/queries/*.sql; do
    echo
    echo "==> Running Q${QUERY_NUM}: ${query}"
    cat "$query"
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches
    yq -i ".result += [[]]" -o json result.json
    for i in $(seq 1 $TRIES); do
        run_query "$QUERY_NUM" "$i" "$query"
    done
    QUERY_NUM=$((QUERY_NUM + 1))
done
