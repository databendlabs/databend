#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

stmt "drop stage if exists s1;"
stmt "create stage s1;"

# one file when #row is small even though multi-threads
echo "copy into @s1/ from (select * from numbers(6000000)) max_file_size=64000000 detailed_output=true" | $BENDSQL_CLIENT_CONNECT | wc -l | sed 's/ //g'

# two files, the larger is about 63569025
echo "copy /*+ set_var(max_threads=1) */ into @s1/ from (select * from numbers(70000000)) max_file_size=64000000 detailed_output=true;"  | $BENDSQL_CLIENT_CONNECT | wc -l | sed 's/ //g'

# one file
echo "copy /*+ set_var(max_threads=1) */ into @s1/ from (select * from numbers(60000000)) max_file_size=64000000 detailed_output=true;"  | $BENDSQL_CLIENT_CONNECT | wc -l | sed 's/ //g'

# one files, limit threads by memory
echo "copy /*+ set_var(max_threads=4) set_var(max_memory_usage=128000000) */ into @s1/ from (select * from numbers(60000000)) max_file_size=64000000 detailed_output=true;"  | $BENDSQL_CLIENT_CONNECT | wc -l | sed 's/ //g'

# two files, limit threads by memory
echo "copy /*+ set_var(max_threads=4) set_var(max_memory_usage=256000000) */ into @s1/ from (select * from numbers(60000000)) max_file_size=64000000 detailed_output=true;"  | $BENDSQL_CLIENT_CONNECT | wc -l | sed 's/ //g'

stmt "drop stage if exists s1;"
