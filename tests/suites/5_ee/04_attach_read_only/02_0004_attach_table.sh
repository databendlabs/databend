#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

stmt "drop table if exists table_from;"
stmt "drop table if exists table_from2;"
stmt "drop table if exists table_to;"
stmt "drop table if exists table_to2;"


## Create table
stmt "create table table_from(a int) 's3://testbucket/data/' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');"

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
echo "attach table table_to 's3://testbucket/data/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $BENDSQL_CLIENT_CONNECT
# If failed will return err msg. Expect it success.
echo "select * from system.columns where table='table_to' ignore_result;" | $BENDSQL_CLIENT_CONNECT
echo "attach table table_to2 's3://testbucket/data/$storage_prefix' connection=(connection_name ='my_conn')" | $BENDSQL_CLIENT_CONNECT


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

echo "##############################"
echo "# implicitly include columns #"
echo "##############################"

stmt "create or replace table base(c1 string, c2 string, c3 string, c4 string) 's3://testbucket/data/' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');"

stmt "insert into base values('c1', 'c2', 'c3', 'c4')"

base_storage_prefix=$(mysql -uroot -h127.0.0.1 -P3307  -e "set global hide_options_in_show_create_table=0;show create table base" | grep -i snapshot_location | awk -F'SNAPSHOT_LOCATION='"'"'|_ss' '{print $2}')

stmt "drop table if exists attach_tbl"

echo "attaching base table"
echo "attach table attach_tbl (c2, c4) 's3://testbucket/data/$base_storage_prefix' connection=(connection_name ='my_conn')" | $BENDSQL_CLIENT_CONNECT

query "select * from attach_tbl"


# the components after columns, i.e. "'ss://... ' connection = ...." will be cut off
query "show create table attach_tbl" | cut -d "'" -f 1

stmt "drop table if exists attach_tbl"

# include non-exist column should fail
echo "attaching non-exists column"
echo "attach table attach_tbl (c2, c_not_exist) 's3://testbucket/data/$base_storage_prefix' connection=(connection_name ='my_conn')" | $BENDSQL_CLIENT_CONNECT



# access modified table
echo "#############################"
echo "## access renamed columns ###"
echo "#############################"

stmt "drop table if exists attach_tbl"
echo "attach table attach_tbl (c2, c4) 's3://testbucket/data/$base_storage_prefix' connection=(connection_name ='my_conn')" | $BENDSQL_CLIENT_CONNECT

stmt "alter table base RENAME COLUMN c2 to c2_new"

echo "'ATTACH' after 'ALTER TABLE RENAME COLUMN' should see the new name of column"
stmt "drop table if exists attach_tbl2"
echo "attach table attach_tbl2 's3://testbucket/data/$base_storage_prefix' connection=(connection_name ='my_conn')" | $BENDSQL_CLIENT_CONNECT
query "desc attach_tbl2"

stmt "insert into base values('c1', 'c2_new', 'c3', 'c4')"

echo "select all should work"
query "select * from attach_tbl order by c2_new"

echo "select c2_new should work"
query "select c2_new from attach_tbl order by c2_new"

echo "##################################"
echo "## drop column from base table ###"
echo "##################################"

# CASE: dropping columns which are NOT included in the attach table, should not matter
stmt "alter table base DROP COLUMN c1"
stmt "delete from base"
stmt "insert into base values('c2_new', 'c3', 'c4')"
echo "select all should work"
query "select * from attach_tbl"

echo "select c2_new should work"
query "select c2_new from attach_tbl order by c2_new"

# CASE: dropping columns which are included in the attach table
stmt "alter table base DROP COLUMN c4"

# if dropped column is accessed, it will fail
echo "select the dropped column will fail"
query "select c4 from attach_tbl"

# but select ALL will return the remaining columns
query "select * from attach_tbl order by c2_new"


stmt "alter table base DROP COLUMN c2_new"
echo "but if all the include columns are dropped, select ALL should fail as well"
query "select * from attach_tbl"

stmt "drop table if exists attach_tbl;"





stmt "drop connection my_conn;"

stmt "drop table if exists table_from;"
stmt "drop table if exists table_from2;"
stmt "drop table if exists table_to;"
stmt "drop table if exists table_to2;"