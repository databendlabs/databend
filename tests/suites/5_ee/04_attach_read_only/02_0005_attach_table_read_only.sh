#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# base table
echo "drop table if exists base" | bendsql_connect_root
echo "drop table if exists attach_read_only" | bendsql_connect_root
echo "create table base as select * from numbers(100)" | bendsql_connect_root

storage_prefix=$(mysql -uroot -h127.0.0.1 -P3307  -e "set global hide_options_in_show_create_table=0;show create table base" | grep -i snapshot_location | awk -F'SNAPSHOT_LOCATION='"'"'|_ss' '{print $2}')

# attach table
echo "attach table attach_read_only 's3://testbucket/admin/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | bendsql_connect_root


#  1. content of two tables should be same
echo "sum of base table"
echo "select sum(number) from base;" | bendsql_connect_root
echo "sum of attach_read_only table"
echo "select sum(number) from attach_read_only;" | bendsql_connect_root

#  2. data should be in-sync
echo "attach table should reflects the mutation of table being attached"
echo "delete from base where number > 0;" | bendsql_connect_root_null
echo "content of base table after deletion"
echo "select * from attach_read_only order by number;" | bendsql_connect_root
echo "content of test attach only table after deletion"
echo "select * from attach_read_only order by number;" | bendsql_connect_root

echo "count() of base table after deletion"
echo "select count() from base;" | bendsql_connect_root
echo "count() of test attach only table"
echo "select count() from attach_read_only;" | bendsql_connect_root

# 3. READ_ONLY attach table should aware of the schema evolution of table being attached
echo "alter table modify column"
echo "alter table base modify column number varchar;" | bendsql_connect_root
echo "expects column number as varchar"
echo "desc attach_read_only;" | bendsql_connect_root
echo "expects one row"
echo "select * from attach_read_only order by number;" | bendsql_connect_root

echo "alter table add column"
echo "alter table base add column c1 varchar NOT NULL DEFAULT 'c1';" | bendsql_connect_root
echo "alter table base add column c2 varchar NOT NULL DEFAULT 'c2';" | bendsql_connect_root
echo "expects 3 columns: number, c1, c2"
echo "desc attach_read_only;" | bendsql_connect_root
echo "expects one row, 3 columns"
echo "select * from attach_read_only order by number;" | bendsql_connect_root

echo "system.columns hides source-side added columns when refresh is disabled (default)"
echo "select name from system.columns where database='default' and table='attach_read_only' order by name;" | $BENDSQL_CLIENT_CONNECT
echo "system.columns should reflect the added columns"
echo "set enable_table_schema_refresh=1; select name from system.columns where database='default' and table='attach_read_only' order by name;" | $BENDSQL_CLIENT_CONNECT
echo "information_schema.columns should reflect the added columns"
echo "set enable_table_schema_refresh=1; select column_name from information_schema.columns where table_schema='default' and table_name='attach_read_only' order by column_name;" | $BENDSQL_CLIENT_CONNECT

echo "refresh does NOT persist to meta server: disable refresh again, should still see frozen schema"
echo "select name from system.columns where database='default' and table='attach_read_only' order by name;" | $BENDSQL_CLIENT_CONNECT


echo "alter table drop column"
echo "alter table base drop column c1;" | bendsql_connect_root
echo "expects new columns: number, c2"
echo "desc attach_read_only;" | bendsql_connect_root
echo "expects one row, 2 columns"
echo "select * from attach_read_only order by number;" | bendsql_connect_root

# 4. READ_ONLY attach table is not allowed to be mutated

# 4.0 basic cases

echo "delete not allowed"
echo "DELETE from attach_read_only" | bendsql_connect_root

echo "update not allowed"
echo "UPDATE attach_read_only set number = 1" | bendsql_connect_root

echo "column not exists"
echo "UPDATE attach_read_only set a = 1" | bendsql_connect_root

echo "truncate not allowed"
echo "TRUNCATE table attach_read_only" | bendsql_connect_root

echo "alter table column not allowed"
echo "ALTER table attach_read_only ADD COLUMN brand_new_col varchar" | bendsql_connect_root

echo "alter table set options not allowed"
echo "ALTER table attach_read_only SET OPTIONS(bloom_index_columns='a');" | bendsql_connect_root

echo "alter table flashback not allowed"
echo "ALTER TABLE attach_read_only FLASHBACK TO (SNAPSHOT => 'c5c538d6b8bc42f483eefbddd000af7d')" | bendsql_connect_root

echo "alter table recluster not allowed"
echo "ALTER TABLE attach_read_only recluster" | bendsql_connect_root


echo "analyze table not allowed"
echo "ANALYZE TABLE attach_read_only" | bendsql_connect_root

echo "optimize table"
echo "optimize table compact not allowed"
echo "OPTIMIZE TABLE attach_read_only compact" | bendsql_connect_root
echo "optimize table compact segment not allowed"
echo "OPTIMIZE TABLE attach_read_only compact segment" | bendsql_connect_root
echo "optimize table purge not allowed"
echo "OPTIMIZE TABLE attach_read_only purge" | bendsql_connect_root

# 4.1 drop table

echo "drop table ALL not allowed"
echo "drop table attach_read_only all" | bendsql_connect_root

echo "drop table IS allowed"
echo "drop table attach_read_only" | bendsql_connect_root

echo "undrop table should work"
echo "undrop table attach_read_only" | bendsql_connect_root
echo "select * from attach_read_only order by number" | bendsql_connect_root


# 4.2 show create table
echo "show create attach table"
# since db_id and table_id varies between executions, replace them with PLACE_HOLDER
# e.g. s3://testbucket/data/1/401/ to s3://testbucket/data/PLACE_HOLDER/PLACE_HOLDER/
echo "show create table attach_read_only" | bendsql_connect_root | sed -E 's/[0-9]+/PLACE_HOLDER/g'

# 4.2 copy into
echo "copy into attached table should fail"
echo "CREATE OR REPLACE STAGE test_attach_source;" | bendsql_connect_root
echo "copy into attach_read_only from @test_attach_source" | bendsql_connect_root


echo "drop table if exists base" | bendsql_connect_root
echo "drop table if exists attach_read_only" | bendsql_connect_root
