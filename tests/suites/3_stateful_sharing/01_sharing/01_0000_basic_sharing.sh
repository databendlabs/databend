#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop test share and database"
echo "drop share if exists test_share" | $MYSQL_CLIENT_SHARE_1_CONNECT
echo "drop share if exists my_share" | $MYSQL_CLIENT_SHARE_2_CONNECT
echo "drop database if exists test_database" | $MYSQL_CLIENT_SHARE_1_CONNECT
echo "drop database if exists shared_db" | $MYSQL_CLIENT_SHARE_2_CONNECT
echo "drop database if exists shared_db" | $MYSQL_CLIENT_SHARE_3_CONNECT

# prepare shared database and table
echo "prepare shared database and table"
echo "CREATE SHARE test_share" | $MYSQL_CLIENT_SHARE_1_CONNECT
echo "CREATE DATABASE test_database" | $MYSQL_CLIENT_SHARE_1_CONNECT
echo "CREATE TABLE test_database.t1 (number UInt64)" | $MYSQL_CLIENT_SHARE_1_CONNECT
echo "INSERT INTO test_database.t1 VALUES (1),(2),(3)" | $MYSQL_CLIENT_SHARE_1_CONNECT
echo "CREATE TABLE test_database.t2 (number UInt64)" | $MYSQL_CLIENT_SHARE_1_CONNECT
echo "INSERT INTO test_database.t2 VALUES (4),(5),(6)" | $MYSQL_CLIENT_SHARE_1_CONNECT
echo "GRANT USAGE ON DATABASE test_database TO SHARE test_share" | $MYSQL_CLIENT_SHARE_1_CONNECT
echo "GRANT SELECT ON TABLE test_database.t1 TO SHARE test_share" | $MYSQL_CLIENT_SHARE_1_CONNECT
echo "GRANT SELECT ON TABLE test_database.t2 TO SHARE test_share" | $MYSQL_CLIENT_SHARE_1_CONNECT
echo "ALTER SHARE test_share ADD TENANTS = shared_tenant,to_tenant" | $MYSQL_CLIENT_SHARE_1_CONNECT
echo "SHOW SHARES" | $MYSQL_CLIENT_SHARE_1_CONNECT | awk '{print $(NF-4), $(NF-3), $(NF-2), $(NF-1), $NF}'

# get shared database and table from same tenant
echo "get shared database and table from same tenant"
echo "SHOW SHARES" | $MYSQL_CLIENT_SHARE_3_CONNECT | awk '{print $(NF-4), $(NF-3), $(NF-2), $(NF-1), $NF}'
echo "CREATE DATABASE if not exists shared_db FROM SHARE shared_tenant.test_share" | $MYSQL_CLIENT_SHARE_3_CONNECT
echo "SELECT * FROM shared_db.t1" | $MYSQL_CLIENT_SHARE_3_CONNECT
echo "SELECT * FROM shared_db.t2" | $MYSQL_CLIENT_SHARE_3_CONNECT

# get shared database and table from another tenant
echo "get shared database and table from another tenant"
echo "drop share endpoint if exists to_share" | $MYSQL_CLIENT_SHARE_2_CONNECT
echo "create share endpoint to_share url='http://127.0.0.1:23003' tenant=shared_tenant" | $MYSQL_CLIENT_SHARE_2_CONNECT
echo "SHOW SHARES" | $MYSQL_CLIENT_SHARE_2_CONNECT | awk '{print $(NF-4), $(NF-3), $(NF-2), $(NF-1), $NF}'
echo "CREATE DATABASE if not exists shared_db FROM SHARE shared_tenant.test_share" | $MYSQL_CLIENT_SHARE_2_CONNECT
echo "SELECT * FROM shared_db.t1" | $MYSQL_CLIENT_SHARE_2_CONNECT
echo "SELECT * FROM shared_db.t2" | $MYSQL_CLIENT_SHARE_2_CONNECT

# cannot grant a database which create from share
echo "cannot grant a database which create from share"
echo "CREATE SHARE my_share" | $MYSQL_CLIENT_SHARE_2_CONNECT
echo "GRANT USAGE ON DATABASE shared_db TO SHARE my_share" | $MYSQL_CLIENT_SHARE_2_CONNECT

# alter table and query table data again
echo "alter table add column and query table data again"
echo "ALTER table test_database.t1 add column b UInt64 default 1" | $MYSQL_CLIENT_SHARE_1_CONNECT
echo "SELECT * FROM shared_db.t1 order by number" | $MYSQL_CLIENT_SHARE_2_CONNECT | awk '{print $(NF-1)","$(NF)}'
echo "SELECT * FROM shared_db.t1 order by number" | $MYSQL_CLIENT_SHARE_3_CONNECT | awk '{print $(NF-1)","$(NF)}'
echo "alter table drop column and query table data again"
echo "ALTER table test_database.t1 drop column number" | $MYSQL_CLIENT_SHARE_1_CONNECT
echo "SELECT * FROM shared_db.t1" | $MYSQL_CLIENT_SHARE_2_CONNECT
echo "SELECT * FROM shared_db.t1" | $MYSQL_CLIENT_SHARE_3_CONNECT

# drop shared table and query data from share
echo "drop shared table and query data from share"
echo "drop table if exists test_database.t1" | $MYSQL_CLIENT_SHARE_1_CONNECT
echo "SELECT * FROM shared_db.t1" | $MYSQL_CLIENT_SHARE_2_CONNECT
echo "SELECT * FROM shared_db.t1" | $MYSQL_CLIENT_SHARE_3_CONNECT

# drop shared database and query data from share
echo "drop shared database and query data from share"
echo "SELECT * FROM shared_db.t2" | $MYSQL_CLIENT_SHARE_2_CONNECT
echo "SELECT * FROM shared_db.t2" | $MYSQL_CLIENT_SHARE_3_CONNECT
echo "drop database if exists test_database" | $MYSQL_CLIENT_SHARE_1_CONNECT
echo "SELECT * FROM shared_db.t2" | $MYSQL_CLIENT_SHARE_2_CONNECT
echo "SELECT * FROM shared_db.t2" | $MYSQL_CLIENT_SHARE_3_CONNECT

## Drop database and share.
echo "all is good"
echo "drop share if exists test_share" | $MYSQL_CLIENT_SHARE_1_CONNECT
echo "drop share if exists my_share" | $MYSQL_CLIENT_SHARE_2_CONNECT
echo "drop database if exists test_database" | $MYSQL_CLIENT_SHARE_1_CONNECT
echo "drop database if exists shared_db" | $MYSQL_CLIENT_SHARE_2_CONNECT
echo "drop database if exists shared_db" | $MYSQL_CLIENT_SHARE_3_CONNECT
