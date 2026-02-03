#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

run_copy_count() {
  echo "$1" | $BENDSQL_CLIENT_CONNECT | wc -l | sed 's/ //g'
}

SINGLE_ROW_COUNT=${COPY_PARQUET_SINGLE_ROW_COUNT:-600000}
DOUBLE_ROW_COUNT=${COPY_PARQUET_DOUBLE_ROW_COUNT:-7000000}
HEAVY_ROW_COUNT=${COPY_PARQUET_HEAVY_ROW_COUNT:-6000000}
SMALL_FILE_SIZE=${COPY_PARQUET_SMALL_FILE_SIZE:-6400000}
DUAL_FILE_SIZE=${COPY_PARQUET_DUAL_FILE_SIZE:-2500000}
MEMORY_LIMIT=${COPY_PARQUET_MEMORY_LIMIT:-128000000}
MEMORY_LIMIT_HEAVY=${COPY_PARQUET_MEMORY_LIMIT_HEAVY:-256000000}
COPY_ITERATIONS=${COPY_PARQUET_ITERATIONS:-5}

stmt "drop stage if exists s1;"
stmt "create stage s1;"

# one file when #row is small even though multi-threads
run_copy_count "copy into @s1/ from (select * from numbers(${SINGLE_ROW_COUNT})) max_file_size=${SMALL_FILE_SIZE} detailed_output=true;"

# two files, the larger is 24960476
run_copy_count "copy /*+ set_var(max_threads=1) */ into @s1/ from (select * from numbers(${DOUBLE_ROW_COUNT})) max_file_size=${DUAL_FILE_SIZE} detailed_output=true;"

# one file
run_copy_count "copy /*+ set_var(max_threads=1) */ into @s1/ from (select * from numbers(${HEAVY_ROW_COUNT})) max_file_size=${SMALL_FILE_SIZE} detailed_output=true;"

# one files, limit threads by memory
run_copy_count "copy /*+ set_var(max_threads=4) set_var(max_memory_usage=${MEMORY_LIMIT}) */ into @s1/ from (select * from numbers(${HEAVY_ROW_COUNT})) max_file_size=${SMALL_FILE_SIZE} detailed_output=true;"

# two files, limit threads by memory
# copy /*+ set_var(max_threads=4) set_var(max_memory_usage=256000000) */ not working in cluster mode
run_copy_count "set max_threads=4; set max_memory_usage=${MEMORY_LIMIT_HEAVY}; copy /*+ set_var(max_threads=4) set_var(max_memory_usage=${MEMORY_LIMIT_HEAVY}) */ into @s1/ from (select * from numbers(${HEAVY_ROW_COUNT})) max_file_size=${SMALL_FILE_SIZE} detailed_output=true;"

stmt "remove @s1;"

for i in `seq 1 ${COPY_ITERATIONS}`;do
	echo "copy into @s1/ from (select number a, number + 1 b from numbers(20))" | $BENDSQL_CLIENT_CONNECT > /dev/null 2>&1
done

stmt "select count() from @s1 where a >= 0 and b <= 1000;"

stmt "remove @s1;"
stmt "drop stage if exists s1;"
