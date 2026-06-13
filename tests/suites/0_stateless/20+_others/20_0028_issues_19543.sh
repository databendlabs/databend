#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

QUERY="""
SELECT /*+ set_var(max_threads=16) */
    (number % 800000)::string AS org_code,
    (number % 800000)::string AS patient_id,
    GROUP_CONCAT(
        DISTINCT concat('wm_name_', (number % 97)::string, '_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'),
        '||'
    ) AS combined_diseases,
    GROUP_CONCAT(
        DISTINCT concat('wm_code_', (number % 89)::string, '_yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy'),
        '||'
    ) AS combined_disease_codes,
    GROUP_CONCAT(
        DISTINCT concat('code_', (number % 83)::string, '_zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'),
        '||'
    ) AS disease_code,
    GROUP_CONCAT(
        DISTINCT concat('name_', (number % 79)::string, '_wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww'),
        '||'
    ) AS disease_name
FROM numbers_mt(3200000)
GROUP BY 1, 2
IGNORE_RESULT;
"""

# Warm up the aggregate path to reduce noise from one-time allocations.
for i in `seq 1 2`; do
    $BENDSQL_CLIENT_CONNECT --query="$QUERY"
done

PIDS=($(pgrep databend-query))
BEFORE_MEM=0
for PID in "${PIDS[@]}"; do
    MEM=$(ps -o rss= -p "$PID" | tail -n 1)
    BEFORE_MEM=$((BEFORE_MEM + MEM))
done

for i in `seq 1 3`; do
    echo "executing $i"
    $BENDSQL_CLIENT_CONNECT --query="$QUERY"
done

sleep 15

AFTER_MEM=0
for PID in "${PIDS[@]}"; do
    MEM=$(ps -o rss= -p "$PID" | tail -n 1)
    AFTER_MEM=$((AFTER_MEM + MEM))
done

DIFF=$(awk -v before="$BEFORE_MEM" -v after="$AFTER_MEM" 'BEGIN {print (after-before)/before * 100}')

if (( $(awk -v diff="$DIFF" 'BEGIN {print (diff < 5)}') )); then
    echo "Memory usage difference is less than 5%"
else
    echo "Memory usage difference is greater than 5%, before ${BEFORE_MEM} ${AFTER_MEM}"
    exit 1
fi
