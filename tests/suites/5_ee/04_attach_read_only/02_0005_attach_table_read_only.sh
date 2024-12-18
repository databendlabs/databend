#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# base table
echo "drop table if exists base" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists attach_read_only" | $BENDSQL_CLIENT_CONNECT
echo "create table base as select * from numbers(100)" | $BENDSQL_CLIENT_CONNECT

storage_prefix=$(mysql -uroot -h127.0.0.1 -P3307  -e "set global hide_options_in_show_create_table=0;show create table base" | grep -i snapshot_location | awk -F'SNAPSHOT_LOCATION='"'"'|_ss' '{print $2}')

# attach table
echo "attach table attach_read_only 's3://testbucket/admin/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $BENDSQL_CLIENT_CONNECT


#  1. content of two tables should be same
echo "sum of base table"
echo "select sum(number) from base;" | $BENDSQL_CLIENT_CONNECT
echo "sum of attach_read_only table"
echo "select sum(number) from attach_read_only;" | $BENDSQL_CLIENT_CONNECT

#  2. data should be in-sync
echo "attach table should reflects the mutation of table being attached"
echo "delete from base where number > 0;" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "content of base table after deletion"
echo "select * from attach_read_only order by number;" | $BENDSQL_CLIENT_CONNECT
echo "content of test attach only table after deletion"
echo "select * from attach_read_only order by number;" | $BENDSQL_CLIENT_CONNECT

echo "count() of base table after deletion"
echo "select count() from base;" | $BENDSQL_CLIENT_CONNECT
echo "count() of test attach only table"
echo "select count() from attach_read_only;" | $BENDSQL_CLIENT_CONNECT

# 3. READ_ONLY attach table should aware of the schema evolution of table being attached
echo "alter table modify column"
echo "alter table base modify column number varchar;" | $BENDSQL_CLIENT_CONNECT
echo "expects column number as varchar"
echo "desc attach_read_only;" | $BENDSQL_CLIENT_CONNECT
echo "expects one row"
echo "select * from attach_read_only order by number;" | $BENDSQL_CLIENT_CONNECT

echo "alter table add column"
echo "alter table base add column c1 varchar NOT NULL DEFAULT 'c1';" | $BENDSQL_CLIENT_CONNECT
echo "alter table base add column c2 varchar NOT NULL DEFAULT 'c2';" | $BENDSQL_CLIENT_CONNECT
echo "expects 3 columns: number, c1, c2"
echo "desc attach_read_only;" | $BENDSQL_CLIENT_CONNECT
echo "expects one row, 3 columns"
echo "select * from attach_read_only order by number;" | $BENDSQL_CLIENT_CONNECT


echo "alter table drop column"
echo "alter table base drop column c1;" | $BENDSQL_CLIENT_CONNECT
echo "expects new columns: number, c2"
echo "desc attach_read_only;" | $BENDSQL_CLIENT_CONNECT
echo "expects one row, 2 columns"
echo "select * from attach_read_only order by number;" | $BENDSQL_CLIENT_CONNECT

# 4. READ_ONLY attach table is not allowed to be mutated

# 4.0 basic cases

echo "delete not allowed"
echo "DELETE from attach_read_only" | $BENDSQL_CLIENT_CONNECT

echo "update not allowed"
echo "UPDATE attach_read_only set a = 1" | $BENDSQL_CLIENT_CONNECT

echo "truncate not allowed"
echo "TRUNCATE table attach_read_only" | $BENDSQL_CLIENT_CONNECT

echo "alter table column not allowed"
echo "ALTER table attach_read_only ADD COLUMN brand_new_col varchar" | $BENDSQL_CLIENT_CONNECT

echo "alter table set options not allowed"
echo "ALTER table attach_read_only SET OPTIONS(bloom_index_columns='a');" | $BENDSQL_CLIENT_CONNECT

echo "alter table flashback not allowed"
echo "ALTER TABLE attach_read_only FLASHBACK TO (SNAPSHOT => 'c5c538d6b8bc42f483eefbddd000af7d')" | $BENDSQL_CLIENT_CONNECT

echo "alter table recluster not allowed"
echo "ALTER TABLE attach_read_only recluster" | $BENDSQL_CLIENT_CONNECT


echo "analyze table not allowed"
echo "ANALYZE TABLE attach_read_only" | $BENDSQL_CLIENT_CONNECT

echo "optimize table"
echo "optimize table compact not allowed"
echo "OPTIMIZE TABLE attach_read_only compact" | $BENDSQL_CLIENT_CONNECT
echo "optimize table compact segment not allowed"
echo "OPTIMIZE TABLE attach_read_only compact segment" | $BENDSQL_CLIENT_CONNECT
echo "optimize table purge not allowed"
echo "OPTIMIZE TABLE attach_read_only purge" | $BENDSQL_CLIENT_CONNECT

# 4.1 drop table

echo "drop table ALL not allowed"
echo "drop table attach_read_only all" | $BENDSQL_CLIENT_CONNECT

echo "drop table IS allowed"
echo "drop table attach_read_only" | $BENDSQL_CLIENT_CONNECT

echo "undrop table should work"
echo "undrop table attach_read_only" | $BENDSQL_CLIENT_CONNECT
echo "select * from attach_read_only order by number" | $BENDSQL_CLIENT_CONNECT


# 4.2 show create table
echo "show create attach table"
# since db_id and table_id varies between executions, replace them with PLACE_HOLDER
# e.g. s3://testbucket/admin/data/1/401/ to s3://testbucket/admin/data/PLACE_HOLDER/PLACE_HOLDER/
echo "show create table attach_read_only" | $BENDSQL_CLIENT_CONNECT | sed -E 's/[0-9]+/PLACE_HOLDER/g'

echo "drop table if exists base" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists attach_read_only" | $BENDSQL_CLIENT_CONNECT
