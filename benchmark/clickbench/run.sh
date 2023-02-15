#!/bin/bash

set -e

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
    if echo "$query" | bendsql query; then
        q_end=$(date +%s.%N)
        q_time=$(echo "$q_end - $q_start" | bc -l)
        echo "Q${QUERY_NUM}[$seq] succeeded in $q_time seconds"
        append_result "$query_num" "$seq" "$q_time"
    else
        echo "Q${QUERY_NUM}[$seq] failed"
        append_result "$query_num" "$seq" "null"
    fi
}

TRIES=6
QUERY_NUM=1
while read -r query; do
    echo "Running Q${QUERY_NUM}: ${query}"
    for i in $(seq 1 $TRIES); do
        run_query "$QUERY_NUM" "$i" "$query"
    done
    QUERY_NUM=$((QUERY_NUM + 1))
done <queries.sql
