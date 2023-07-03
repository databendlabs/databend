#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

## Setup
echo "drop database if exists test_vacuum_drop" | $MYSQL_CLIENT_CONNECT

echo "CREATE DATABASE test_vacuum_drop" | $MYSQL_CLIENT_CONNECT
echo "create table test_vacuum_drop.a(c int)" | $MYSQL_CLIENT_CONNECT

echo "INSERT INTO test_vacuum_drop.a VALUES (1)" | $MYSQL_CLIENT_CONNECT

echo "drop table test_vacuum_drop.a" | $MYSQL_CLIENT_CONNECT

echo "vacuum drop table from test_vacuum_drop retain 0 hours" | $MYSQL_CLIENT_CONNECT

echo "undrop table test_vacuum_drop.a" | $MYSQL_CLIENT_CONNECT

echo "select * from test_vacuum.a" | $MYSQL_CLIENT_CONNECT

echo "create table test_vacuum_drop.b(c int)" | $MYSQL_CLIENT_CONNECT

echo "INSERT INTO test_vacuum_drop.b VALUES (1)" | $MYSQL_CLIENT_CONNECT

echo "vacuum drop table from test_vacuum_drop" | $MYSQL_CLIENT_CONNECT

echo "undrop table test_vacuum_drop.b" | $MYSQL_CLIENT_CONNECT

echo "select * from test_vacuum.b" | $MYSQL_CLIENT_CONNECT

echo "drop database if exists test_vacuum_drop" | $MYSQL_CLIENT_CONNECT
