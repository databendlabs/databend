#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists table_block_per_segment;" | bendsql_connect_root

## Create table
echo "create table table_block_per_segment(a int) block_per_segment = 2000;" | bendsql_connect_root
echo "create table table_block_per_segment(a int) block_per_segment = 500;" | bendsql_connect_root

## Drop table
echo "drop table if exists table_block_per_segment;" | bendsql_connect_root
