#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


## warmup
for i in `seq 1 2`;do
	$BENDSQL_CLIENT_CONNECT --query="""
	select number::string , max(number::string), min(number::string), count(distinct number) from numbers(10000000) group by 1 ignore_result;
	"""
done


PIDS=($(pgrep databend-query))
BEFORE_MEM=0
for PID in "${PIDS[@]}"; do
    MEM=$(ps -o rss= -p $PID | tail -n 1)
    BEFORE_MEM=$((BEFORE_MEM + MEM))
done


for i in `seq 1 5`;do
	echo "executing $i"
	$BENDSQL_CLIENT_CONNECT --query="""
	select number::string , max(number::string), min(number::string), count(distinct number) from numbers(10000000) group by 1 ignore_result;
	"""
done

sleep 15


AFTER_MEM=0
for PID in "${PIDS[@]}"; do
    MEM=$(ps -o rss= -p $PID | tail -n 1)
    AFTER_MEM=$((AFTER_MEM + MEM))
done

# Calculate the difference in percentage
DIFF=$(awk -v before=$BEFORE_MEM -v after=$AFTER_MEM 'BEGIN {print (after-before)/before * 100}')

# Check if the difference is less than 5%
if (( $(awk -v diff=$DIFF 'BEGIN {print (diff < 5)}') )); then
    echo "Memory usage difference is less than 5%"
else
    echo "Memory usage difference is greater than 5%, before ${BEFORE_MEM} ${AFTER_MEM}"
fi