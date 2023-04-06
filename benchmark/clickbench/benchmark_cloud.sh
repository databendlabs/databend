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
python3 --version
yq --version
bendsql version

echo "Preparing benchmark metadata..."
echo '{}' >result.json
yq -i ".date = \"$(date -u +%Y-%m-%d)\"" result.json
yq -i '.tags = ["s3"]' result.json
case ${BENCHMARK_SIZE} in
Medium)
    yq -i '.cluster_size = "16"' result.json
    yq -i '.machine = "16×Medium"' result.json
    ;;
Large)
    yq -i '.cluster_size = "64"' result.json
    yq -i '.machine = "64×Large"' result.json
    ;;
*)
    echo "Unsupported benchmark size: ${BENCHMARK_SIZE}"
    exit 1
    ;;
esac

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

echo "Running queries..."

function run_query() {
    local query_num=$1
    local seq=$2
    local query=$3

    local q_start q_end q_time

    q_start=$(date +%s.%N)
    if echo "$query" | bendsql query --format csv --rows-only >/dev/null; then
        q_end=$(date +%s.%N)
        q_time=$(python3 -c "print($q_end - $q_start)")
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
    yq -i ".result += [[]]" result.json
    for i in $(seq 1 $TRIES); do
        run_query "$QUERY_NUM" "$i" "$query"
    done
    QUERY_NUM=$((QUERY_NUM + 1))
done <"${BENCHMARK_DATASET}/queries.sql"

echo "Cleaning up..."
bendsql cloud warehouse delete "${CLOUD_WAREHOUSE}"
