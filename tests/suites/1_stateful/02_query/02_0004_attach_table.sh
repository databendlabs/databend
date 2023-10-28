#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists table_from;" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists table_to;" | $MYSQL_CLIENT_CONNECT

## Create table
echo "create table table_from(a int) 's3://testbucket/admin/data/' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $MYSQL_CLIENT_CONNECT

table_inserts=(
  "insert into table_from(a) values(0)"
  "insert into table_from(a) values(1)"
  "insert into table_from(a) values(2)"
)

for i in "${table_inserts[@]}"; do
  echo "$i" | $MYSQL_CLIENT_CONNECT
done

storage_prefix=$(mysql -uroot -h127.0.0.1 -P3307  -e "set global hide_options_in_show_create_table=0;show create table table_from" | grep -i snapshot_location | awk -F'SNAPSHOT_LOCATION='"'"'|_ss' '{print $2}')

echo "attach table table_to 's3://testbucket/admin/data/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $MYSQL_CLIENT_CONNECT


# ## Select table
echo "select * from table_to order by a;" | $MYSQL_CLIENT_CONNECT

echo "delete from table_to where a=1;" | $MYSQL_CLIENT_CONNECT

echo "rows after deletion"
echo "select * from table_to order by a;" | $MYSQL_CLIENT_CONNECT


# READ_ONLY attach

echo "Attach READ_ONLY"
# fetch table storage prefix
storage_prefix=$(mysql -uroot -h127.0.0.1 -P3307  -e "set global hide_options_in_show_create_table=0;show create table table_from" | grep -i snapshot_location | awk -F'SNAPSHOT_LOCATION='"'"'|_ss' '{print $2}')

# 1. READ_ONLY attach table
echo "attach table table_read_only 's3://testbucket/admin/data/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}') READ_ONLY;" | $MYSQL_CLIENT_CONNECT

echo "check content of attach table"
# ## check table content
echo "select * from table_read_only order by a;" | $MYSQL_CLIENT_CONNECT

# 2. READ_ONLY attach table should reflects the mutation of table being attached
# del from the attachED table
echo "delete from table_from where a=2;" | $MYSQL_CLIENT_CONNECT

echo "check content of attach table, after row has been deleted from attachED table:"
echo "  there should be only one row"
# 2.1 check table content
echo "select * from table_read_only order by a;" | $MYSQL_CLIENT_CONNECT

# 2.1 check count() table
echo "count of the original table after deletion is 1"
echo "select count() from table_from" | $MYSQL_CLIENT_CONNECT
echo "count of the attach read_only table should also be 1"
echo "select count() from table_read_only" | $MYSQL_CLIENT_CONNECT

# 3. READ_ONLY attach table should aware of the schema evolution of table being attached
# TODO currently, there is a design issue blocking this feature (the constructor of table is sync style)
# will be implemented in later PR
#echo "alter table table_to add column new_col bigint default 10" | $MYSQL_CLIENT_CONNECT
#
#echo "check table after new column has been added to attachED table"
#echo "select * from table_read_only order by a;" | $MYSQL_CLIENT_CONNECT

# 4. READ_ONLY attach table is not allowed to be mutated

# 4.0 basic cases

echo "delete not allowed"
echo "DELETE from table_read_only" | $MYSQL_CLIENT_CONNECT

echo "update not allowed"
echo "UPDATE table_read_only set a = 1" | $MYSQL_CLIENT_CONNECT

echo "truncate not allowed"
echo "TRUNCATE table table_read_only" | $MYSQL_CLIENT_CONNECT

echo "alter table column not allowed"
echo "ALTER table table_read_only ADD COLUMN brand_new_col varchar" | $MYSQL_CLIENT_CONNECT

echo "alter table set options not allowed"
echo "ALTER table table_read_only SET OPTIONS(bloom_index_columns='a');" | $MYSQL_CLIENT_CONNECT

echo "alter table flashback not allowed"
echo "ALTER TABLE table_read_only FLASHBACK TO (SNAPSHOT => 'c5c538d6b8bc42f483eefbddd000af7d')" | $MYSQL_CLIENT_CONNECT

echo "alter table recluster not allowed"
echo "ALTER TABLE table_read_only recluster" | $MYSQL_CLIENT_CONNECT


echo "analyze table not allowed"
echo "ANALYZE TABLE table_read_only" | $MYSQL_CLIENT_CONNECT

echo "optimize table"
echo "optimize table compact not allowed"
echo "OPTIMIZE TABLE table_read_only compact" | $MYSQL_CLIENT_CONNECT
echo "optimize table compact segment not allowed"
echo "OPTIMIZE TABLE table_read_only compact segment" | $MYSQL_CLIENT_CONNECT
echo "optimize table purge not allowed"
echo "OPTIMIZE TABLE table_read_only purge" | $MYSQL_CLIENT_CONNECT

# 4.1 drop table

echo "drop table ALL not allowed"
echo "drop table table_read_only all" | $MYSQL_CLIENT_CONNECT

echo "drop table IS allowed"
echo "drop table table_read_only" | $MYSQL_CLIENT_CONNECT

echo "undrop table should work"
echo "undrop table table_read_only" | $MYSQL_CLIENT_CONNECT
echo "select * from table_read_only order by a" | $MYSQL_CLIENT_CONNECT


# 4.2 show create table
echo "show create attach table"
# since db_id and table_id varies between executions, replace them with PLACE_HOLDER
# e.g. s3://testbucket/admin/data/1/401/ to s3://testbucket/admin/data/PLACE_HOLDER/PLACE_HOLDER/
echo "show create table table_read_only" | $MYSQL_CLIENT_CONNECT | sed 's/[0-9]\+/PLACE_HOLDER/g'

