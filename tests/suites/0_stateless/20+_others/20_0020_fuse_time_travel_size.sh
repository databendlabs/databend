#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

stmt "create or replace database test_fuse_time_travel_size" 

rm -rf /tmp/test_fuse_time_travel_size/

mkdir -p /tmp/test_fuse_time_travel_size/

chmod 777 /tmp/test_fuse_time_travel_size/

stmt "create table test_fuse_time_travel_size.t(c int) 'fs:///tmp/test_fuse_time_travel_size/'" 

stmt "insert into test_fuse_time_travel_size.t values (1),(2)" 

result_size=$(echo "select time_travel_size from fuse_time_travel_size('test_fuse_time_travel_size')" | $BENDSQL_CLIENT_CONNECT)

expected_size=$(find /tmp/test_fuse_time_travel_size/ -type f -exec du -b  {} + | awk '{sum += $1} END {print sum}')

echo $result_size
echo $expected_size

