#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists table_from;" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists table_from2;" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists table_to;" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists table_to2;" | $BENDSQL_CLIENT_CONNECT

query() {
	echo "++++ $1"
	echo "$2"
	echo "----"
	echo "$2" | $BENDSQL_CLIENT_CONNECT
	echo "++++"
}

## Create table
echo "create table table_from(a int) 's3://testbucket/admin/data/' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $BENDSQL_CLIENT_CONNECT

## used self-defined connection
echo "drop connection if exists my_conn ;" | $BENDSQL_CLIENT_CONNECT
echo "create connection my_conn storage_type = 's3' access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}'" | $BENDSQL_CLIENT_CONNECT

table_inserts=(
  "insert into table_from(a) values(0)"
  "insert into table_from(a) values(1)"
  "insert into table_from(a) values(2)"
)

for i in "${table_inserts[@]}"; do
  echo "$i" | $BENDSQL_CLIENT_CONNECT
done

storage_prefix=$(mysql -uroot -h127.0.0.1 -P3307  -e "set global hide_options_in_show_create_table=0;show create table table_from" | grep -i snapshot_location | awk -F'SNAPSHOT_LOCATION='"'"'|_ss' '{print $2}')

echo "attach table table_to 's3://testbucket/admin/data/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $BENDSQL_CLIENT_CONNECT
echo "attach table table_to2 's3://testbucket/admin/data/$storage_prefix' connection=(connection_name ='my_conn')" | $BENDSQL_CLIENT_CONNECT


# ## Select table
query "attach table" "select * from table_to order by a;"

query "attach table with self-defined connection" "select * from table_to2 order by a;"

echo "delete from table_to where a=1;" | $BENDSQL_CLIENT_CONNECT

query "rows after deletion" "select * from table_to order by a;"

query "rows after deletion (self-defined connection)" "select * from table_to2 order by a;"

echo "drop connection my_conn;" | $BENDSQL_CLIENT_CONNECT
