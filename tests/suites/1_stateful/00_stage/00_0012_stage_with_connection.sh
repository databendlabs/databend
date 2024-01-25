#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

stmt "drop table if exists my_table;"
stmt "create table my_table (a int);"
stmt "insert into my_table values (1), (2), (4);"

stmt "drop stage if exists my_stage;"
stmt "drop connection if exists my_conn;"
stmt "create connection my_conn storage_type = 's3' access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}'"
stmt "create stage my_stage url= 's3://testbucket/admin/tempdata/' connection = (connection_name='my_conn');"
stmt "remove @my_stage;"
stmt "copy into @my_stage/a.csv from my_table"
query "select * from @my_stage order by a;"

stmt "drop table if exists my_table;"
stmt "drop stage if exists my_stage;"
stmt "drop connection if exists my_conn;"
