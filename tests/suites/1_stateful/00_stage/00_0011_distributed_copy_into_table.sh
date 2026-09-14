#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "set global max_threads = 1" | bendsql_connect_root
echo "drop table if exists products;" | bendsql_connect_root
echo "drop stage if exists s0011;" | bendsql_connect_root
echo "CREATE STAGE s0011 FILE_FORMAT = (TYPE = CSV);" | bendsql_connect_root
echo "create table products (id int, name string, description string);" | bendsql_connect_root

#multi files to trigger distributed test
curl -s -u root: -H "x-databend-stage-name:s0011" -F "upload=@${CURDIR}/../../../data/csv/itt.csv" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root: | jq ".data"
curl -s -u root: -H "x-databend-stage-name:s0011" -F "upload=@${CURDIR}/../../../data/csv/sample.csv" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root: | jq ".data"
curl -s -u root: -H "x-databend-stage-name:s0011" -F "upload=@${CURDIR}/../../../data/csv/sample_3_replace.csv" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root: | jq ".data"
curl -s -u root: -H "x-databend-stage-name:s0011" -F "upload=@${CURDIR}/../../../data/csv/sample_3_duplicate.csv" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root: | jq ".data"

echo "set enable_distributed_copy_into = 1;copy into products from @s0011 pattern = '.*[.]csv';" | bendsql_connect_root
echo "select * from products order by id,name,description;" | bendsql_connect_root
echo "select count(*) from products;" | bendsql_connect_root
## error test!!!
curl -s -u root: -H "x-databend-stage-name:s0011" -F "upload=@${CURDIR}/../../../data/csv/sample_2_columns.csv" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root: | jq ".data"
echo "set enable_distributed_copy_into = 1;copy into products from @s0011 pattern = '.*[.]csv' purge = true;"  | bendsql_connect_root 2>&1
echo "select count(*) from products;" | bendsql_connect_root
echo "list @s0011;" | bendsql_connect_root | grep -o 'csv' | wc -l | sed 's/ //g'
echo "set enable_distributed_copy_into = 1;copy into products from (select \$1,\$2,\$3 from @s0011 as t2) force = true purge = true;"  | bendsql_connect_root
echo "select count(*) from products;" | bendsql_connect_root
echo "list @s0011;" | bendsql_connect_root | grep -o 'csv' | wc -l | sed 's/ //g'