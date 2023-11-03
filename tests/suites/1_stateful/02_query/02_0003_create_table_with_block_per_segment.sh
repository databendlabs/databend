#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists table_block_per_segment;" | $BENDSQL_CLIENT_CONNECT

## Create table
echo "create table table_block_per_segment(a int) block_per_segment = 2000;" | $BENDSQL_CLIENT_CONNECT
echo "create table table_block_per_segment(a int) block_per_segment = 500;" | $BENDSQL_CLIENT_CONNECT

## Drop table
echo "drop table if exists table_block_per_segment;" | $BENDSQL_CLIENT_CONNECT
