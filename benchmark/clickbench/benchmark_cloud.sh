#!/bin/bash

set -e

BENCHMARK_ID=${BENCHMARK_ID:-$(date +%s)}
BENCHMARK_DATASET=${BENCHMARK_DATASET:-hits}
BENCHMARK_SIZE=${BENCHMARK_SIZE:-Medium}
BENCHMARK_IMAGE_TAG=${BENCHMARK_IMAGE_TAG:-}
BENCHMARK_DATABASE=${BENCHMARK_DATABASE:-default}

if [[ -z "${BENCHMARK_IMAGE_TAG}" ]]; then
    echo "Please set BENCHMARK_IMAGE_TAG to run the benchmark."
    exit 1
fi

CLOUD_EMAIL=${CLOUD_EMAIL:-}
CLOUD_PASSWORD=${CLOUD_PASSWORD:-}
CLOUD_ORG=${CLOUD_ORG:-}
CLOUD_ENDPOINT=${CLOUD_ENDPOINT:-https://app.databend.com}
CLOUD_WAREHOUSE=${CLOUD_WAREHOUSE:-benchmark-${BENCHMARK_ID}}

if [[ -z "${CLOUD_EMAIL}" || -z "${CLOUD_PASSWORD}" || -z "${CLOUD_ORG}" ]]; then
    echo "Please set CLOUD_EMAIL, CLOUD_PASSWORD and CLOUD_ORG to run the benchmark."
    exit 1
fi

echo "Checking script dependencies..."
bc --version
jq --version
bendsql version

echo "#######################################################"
echo "Running benchmark for Databend Cloud with S3 storage..."

# Connect to databend-query
bendsql cloud login \
    --endpoint "${CLOUD_ENDPOINT}" \
    --email "${CLOUD_EMAIL}" \
    --password "${CLOUD_PASSWORD}" \
    --org "${CLOUD_ORG}" \
    --database "${BENCHMARK_DATABASE}"

bendsql cloud warehouse ls
bendsql cloud warehouse create "${CLOUD_WAREHOUSE}" --size "${BENCHMARK_SIZE}" --tag "${BENCHMARK_IMAGE_TAG}"
bendsql cloud warehouse ls
bendsql cloud warehouse resume "${CLOUD_WAREHOUSE}" --wait

bendsql cloud warehouse use "${CLOUD_WAREHOUSE}"

# Data loaded before the benchmark starts
# bendsql query <"${BENCHMARK_DATASET}/create.sql"

# echo "Loading data..."
# load_start=$(date +%s)
# bendsql query <"${BENCHMARK_DATASET}/load.sql"
# load_end=$(date +%s)
# load_time=$(echo "$load_end - $load_start" | bc -l)
# echo "Data loaded in ${load_time}s."

# data_size=$(echo "select sum(data_compressed_size) from system.tables where database = '${BENCHMARK_DATASET}';" | bendsql query -f unaligned -t)

# echo '{}' >result.json
# jq ".load_time = ${load_time} | .data_size = ${data_size} | .result = []" <result.json >result.json.tmp && mv result.json.tmp result.json

echo '{}' >result.json
echo "Running queries..."

function append_result() {
    local query_num=$1
    local seq=$2
    local value=$3
    if [[ $seq -eq 1 ]]; then
        jq ".result += [[${value}]]" <result.json >result.json.tmp && mv result.json.tmp result.json
    else
        jq ".result[${query_num} - 1] += [${value}]" <result.json >result.json.tmp && mv result.json.tmp result.json
    fi
}

function run_query() {
    local query_num=$1
    local seq=$2
    local query=$3

    local q_start q_end q_time

    q_start=$(date +%s.%N)
    if echo "$query" | bendsql query --format csv --rows-only >/dev/null; then
        q_end=$(date +%s.%N)
        q_time=$(echo "$q_end - $q_start" | bc -l)
        echo "Q${QUERY_NUM}[$seq] succeeded in $q_time seconds"
        append_result "$query_num" "$seq" "$q_time"
    else
        echo "Q${QUERY_NUM}[$seq] failed"
        append_result "$query_num" "$seq" "null"
    fi
}

TRIES=5
QUERY_NUM=1
while read -r query; do
    echo "Running Q${QUERY_NUM}: ${query}"
    for i in $(seq 1 $TRIES); do
        run_query "$QUERY_NUM" "$i" "$query"
    done
    QUERY_NUM=$((QUERY_NUM + 1))
done <"${BENCHMARK_DATASET}/queries.sql"

echo "Cleaning up..."
bendsql cloud warehouse delete "${CLOUD_WAREHOUSE}"
