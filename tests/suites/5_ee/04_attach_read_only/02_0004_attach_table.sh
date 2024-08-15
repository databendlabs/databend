#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

stmt "drop table if exists table_from;"
stmt "drop table if exists table_from2;"
stmt "drop table if exists table_to;"
stmt "drop table if exists table_to2;"


## Create table
stmt "create table table_from(a int) 's3://testbucket/admin/data/' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');"

## used self-defined connection
stmt "drop connection if exists my_conn;"
stmt "create connection my_conn storage_type = 's3' access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}'"

table_inserts=(
  "insert into table_from(a) values(0)"
  "insert into table_from(a) values(1)"
  "insert into table_from(a) values(2)"
)

for i in "${table_inserts[@]}"; do
  stmt "$i"
done

storage_prefix=$(mysql -uroot -h127.0.0.1 -P3307  -e "set global hide_options_in_show_create_table=0;show create table table_from" | grep -i snapshot_location | awk -F'SNAPSHOT_LOCATION='"'"'|_ss' '{print $2}')

comment "attaching table"
echo "attach table table_to 's3://testbucket/admin/data/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $BENDSQL_CLIENT_CONNECT
echo "attach table table_to2 's3://testbucket/admin/data/$storage_prefix' connection=(connection_name ='my_conn')" | $BENDSQL_CLIENT_CONNECT


# ## Select table
comment "select attach table"
query "select * from table_to order by a;"

comment "select attach table from system.tables"
query "select is_attach from system.tables where name = 'table_to';"

comment "select attach table with self-defined connection"
query "select * from table_to2 order by a;"


comment "delete should fail"
stmt "delete from table_to where a=1;"

comment "select after deletion"
query "select * from table_to order by a;"

comment "select after deletion with self-defined connection"
query "select * from table_to2 order by a;"

stmt "drop connection my_conn;"

stmt "drop table if exists table_from;"
stmt "drop table if exists table_from2;"
stmt "drop table if exists table_to;"
stmt "drop table if exists table_to2;"