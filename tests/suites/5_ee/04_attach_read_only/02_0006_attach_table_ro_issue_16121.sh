#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "create or replace database issue_16121" | $BENDSQL_CLIENT_CONNECT

echo "create or replace table issue_16121.base as select * from numbers(100)" | $BENDSQL_CLIENT_CONNECT

storage_prefix=$(mysql -uroot -h127.0.0.1 -P3307  -e "set hide_options_in_show_create_table=0;show create table issue_16121.base" | grep -i snapshot_location | awk -F'SNAPSHOT_LOCATION='"'"'|_ss' '{print $2}')
echo "attach table issue_16121.attach_read_only 's3://testbucket/admin/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $BENDSQL_CLIENT_CONNECT

echo "drop table issue_16121.base;" | $BENDSQL_CLIENT_CONNECT

# purge base table data
echo "set data_retention_time_in_days=0;vacuum drop table from issue_16121;" | $BENDSQL_CLIENT_CONNECT

echo "expects no error(nothing outputs)"
echo "drop table issue_16121.attach_read_only" | $BENDSQL_CLIENT_CONNECT

# AI configurations (provider url, ak/sk) are not ready for CI, thus the following test case is commented out
#echo "expects ai_to_sql works"
#echo "SELECT* FROM  ai_to_sql ('current time') IGNORE_RESULT" | $BENDSQL_CLIENT_CONNECT
