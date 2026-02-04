#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

stmt "drop stage if exists s1;"
stmt "create stage s1;"

# one file when #row is small even though multi-threads
echo "copy into @s1/ from (select * from numbers(6000000)) max_file_size=64000000 detailed_output=true" | $BENDSQL_CLIENT_CONNECT | wc -l | sed 's/ //g'

# two files, the larger is 24960476
echo "copy /*+ set_var(max_threads=1) */ into @s1/ from (select * from numbers(70000000)) max_file_size=25000000 detailed_output=true;"  | $BENDSQL_CLIENT_CONNECT | wc -l | sed 's/ //g'

# one file
echo "copy /*+ set_var(max_threads=1) */ into @s1/ from (select * from numbers(60000000)) max_file_size=64000000 detailed_output=true;"  | $BENDSQL_CLIENT_CONNECT | wc -l | sed 's/ //g'

# one files, limit threads by memory
echo "copy /*+ set_var(max_threads=4) set_var(max_memory_usage=128000000) */ into @s1/ from (select * from numbers(60000000)) max_file_size=64000000 detailed_output=true;"  | $BENDSQL_CLIENT_CONNECT | wc -l | sed 's/ //g'

# two files, limit threads by memory
# copy /*+ set_var(max_threads=4) set_var(max_memory_usage=256000000) */ not working in cluster mode
echo "set max_threads=4; set max_memory_usage=256000000; copy /*+ set_var(max_threads=4) set_var(max_memory_usage=256000000) */ into @s1/ from (select * from numbers(60000000)) max_file_size=64000000 detailed_output=true;"  | $BENDSQL_CLIENT_CONNECT | wc -l | sed 's/ //g'

stmt "remove @s1;"

for i in `seq 1 50`;do
	echo "copy into @s1/ from (select number a, number + 1 b from numbers(20))" | $BENDSQL_CLIENT_CONNECT > /dev/null 2>&1
done

stmt "select count() from @s1 where a >= 0 and b <= 1000;"

stmt "remove @s1;"

stmt "create or replace table copy_stage_partition(dt DATE, ts TIMESTAMP, val INT);"
stmt "insert into copy_stage_partition VALUES ('2020-01-28', '2020-01-28 18:00:00', 1), ('2020-01-28', '2020-01-28 22:00:00', 2), ('2020-01-29', '2020-01-29 02:00:00', 3), (NULL, NULL, 4);"

stmt "COPY INTO @s1 FROM copy_stage_partition PARTITION BY ('date=' || to_varchar(dt, 'YYYY-MM-DD') || '/hour=' || lpad(to_varchar(date_part('hour', ts)), 2, '0')) FILE_FORMAT = (type = PARQUET);"

stmt "select count_if(name LIKE 'date=2020-01-28/hour=18/%'), count_if(name LIKE 'date=2020-01-28/hour=22/%'), count_if(name LIKE 'date=2020-01-29/hour=02/%'), count_if(name LIKE '_NULL_/%') from list_stage(location=>'@s1');"

stmt "drop table if exists copy_stage_partition;"

stmt "drop stage if exists s1;"
