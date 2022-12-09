#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop share if exists test_share" | $MYSQL_CLIENT_CONNECT
echo "drop database if exists test_database" | $MYSQL_CLIENT_CONNECT
echo "drop database if exists shared"  | $SHARING_MYSQL_CLIENT_CONNECT

# prepare shared database and table
echo "CREATE SHARE test_share" | $MYSQL_CLIENT_CONNECT
echo "CREATE DATABASE test_database" | $MYSQL_CLIENT_CONNECT
echo "CREATE TABLE test_database.t1 (number UInt64)" | $MYSQL_CLIENT_CONNECT
echo "INSERT INTO test_database.t1 VALUES (1),(2),(3)" | $MYSQL_CLIENT_CONNECT
echo "GRANT USAGE ON DATABASE test_database TO SHARE test_share" | $MYSQL_CLIENT_CONNECT
echo "GRANT SELECT ON TABLE test_database.t1 TO SHARE test_share" | $MYSQL_CLIENT_CONNECT
echo "ALTER SHARE test_share ADD TENANTS = shared_tenant" | $MYSQL_CLIENT_CONNECT
echo "SHOW SHARES" | $MYSQL_CLIENT_CONNECT | awk '{print $(NF-4), $(NF-3), $(NF-2), $(NF-1), $NF}'

# get shared database and table from another tenant
echo "SHOW SHARES" | $SHARING_MYSQL_CLIENT_CONNECT | awk '{print $(NF-4), $(NF-3), $(NF-2), $(NF-1), $NF}'
# bug: show database has no shared database but it still exists when create database
echo "CREATE DATABASE if not exists shared FROM SHARE test_tenant.test_share" | $SHARING_MYSQL_CLIENT_CONNECT
echo "SELECT * FROM shared.t1" | $SHARING_MYSQL_CLIENT_CONNECT


## Drop table.
echo "drop database if exists shared"  | $SHARING_MYSQL_CLIENT_CONNECT
echo "drop share if exists test_share" | $MYSQL_CLIENT_CONNECT
echo "drop database if exists test_database" | $MYSQL_CLIENT_CONNECT
