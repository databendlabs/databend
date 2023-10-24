#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists table_from;" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists table_to;" | $MYSQL_CLIENT_CONNECT

## Create table
echo "create table table_from(a int) 's3://testbucket/admin/data/' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $MYSQL_CLIENT_CONNECT

table_inserts=(
  "insert into table_from(a) values(0)"
  "insert into table_from(a) values(1)"
  "insert into table_from(a) values(2)"
)

for i in "${table_inserts[@]}"; do
  echo "$i" | $MYSQL_CLIENT_CONNECT
done

storage_prefix=$(mysql -uroot -h127.0.0.1 -P3307  -e "set global hide_options_in_show_create_table=0;show create table table_from" | grep -i snapshot_location | awk -F'SNAPSHOT_LOCATION='"'"'|_ss' '{print $2}')

echo "attach table table_to 's3://testbucket/admin/data/$storage_prefix' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $MYSQL_CLIENT_CONNECT


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
echo "attach table table_read_only 's3://testbucket/admin/data/$storage_prefix' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}') READ_ONLY;" | $MYSQL_CLIENT_CONNECT

echo "check content of attach table"
# ## check table content
echo "select * from table_read_only order by a;" | $MYSQL_CLIENT_CONNECT

# 2. READ_ONLY attach table should reflects the mutation of table being attached
# del from the attachED table
echo "delete from table_to where a=2;" | $MYSQL_CLIENT_CONNECT

echo "check content of attach table, after row has been deleted from attachED table:"
echo "  there should be only one row"
# ## check table content
echo "select * from table_read_only order by a;" | $MYSQL_CLIENT_CONNECT

# 3. READ_ONLY attach table should aware of the schema evolution of table being attached
# TODO currently, there is a design issue blocking this feature (the constructor of table is sync style)
# will be implemented in later PR
#echo "alter table table_to add column new_col bigint default 10" | $MYSQL_CLIENT_CONNECT
#
#echo "check table after new column has been added to attachED table"
#echo "select * from table_read_only order by a;" | $MYSQL_CLIENT_CONNECT

# 4. READ_ONLY attach table is not allowed to be mutated

delete from table_read_only;
update table_read_only set a=10 where a=0;
