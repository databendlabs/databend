#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "set global max_threads = 1" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists products;" | $BENDSQL_CLIENT_CONNECT
echo "drop stage if exists s0011;" | $BENDSQL_CLIENT_CONNECT
echo "CREATE STAGE s0011 FILE_FORMAT = (TYPE = CSV);" | $BENDSQL_CLIENT_CONNECT
echo "create table products (id int, name string, description string);" | $BENDSQL_CLIENT_CONNECT

#multi files to trigger distributed test
curl -s -u root: -H "x-databend-stage-name:s0011" -F "upload=@${CURDIR}/../../../data/csv/itt.csv" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root: | jq ".data"
curl -s -u root: -H "x-databend-stage-name:s0011" -F "upload=@${CURDIR}/../../../data/csv/sample.csv" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root: | jq ".data"
curl -s -u root: -H "x-databend-stage-name:s0011" -F "upload=@${CURDIR}/../../../data/csv/sample_3_replace.csv" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root: | jq ".data"
curl -s -u root: -H "x-databend-stage-name:s0011" -F "upload=@${CURDIR}/../../../data/csv/sample_3_duplicate.csv" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root: | jq ".data"

echo "set enable_distributed_copy_into = 1;copy into products from @s0011 pattern = '.*[.]csv';" | $BENDSQL_CLIENT_CONNECT
echo "select * from products order by id,name,description;" | $BENDSQL_CLIENT_CONNECT
echo "select count(*) from products;" | $BENDSQL_CLIENT_CONNECT
## error test!!!
curl -s -u root: -H "x-databend-stage-name:s0011" -F "upload=@${CURDIR}/../../../data/csv/sample_2_columns.csv" -XPUT "http://localhost:8000/v1/upload_to_stage" -u root: | jq ".data"
echo "set enable_distributed_copy_into = 1;copy into products from @s0011 pattern = '.*[.]csv' purge = true;"  | $BENDSQL_CLIENT_CONNECT 2>&1
echo "select count(*) from products;" | $BENDSQL_CLIENT_CONNECT
echo "list @s0011;" | $BENDSQL_CLIENT_CONNECT | grep -o 'csv' | wc -l | sed 's/ //g'
echo "set enable_distributed_copy_into = 1;copy into products from (select \$1,\$2,\$3 from @s0011 as t2) force = true purge = true;"  | $BENDSQL_CLIENT_CONNECT
echo "select count(*) from products;" | $BENDSQL_CLIENT_CONNECT
echo "list @s0011;" | $BENDSQL_CLIENT_CONNECT | grep -o 'csv' | wc -l | sed 's/ //g'