#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

stmt "insert into test_vacuum2 values(2);"

stmt "set data_retention_time_in_days = 2;truncate table test_vacuum2;"

# gc root
stmt "insert into test_vacuum2 values(3);"

# should have 4 snapshots
query "select count(*) from fuse_snapshot('default','test_vacuum2')"

stmt "set data_retention_time_in_days = 0;call system\$fuse_vacuum2('default','test_vacuum2');"

# only the current snapshot remains
query "select count(*) from fuse_snapshot('default','test_vacuum2')"

# verify the data
query "select * from test_vacuum2;"

# restore default value
stmt "set data_retention_time_in_days = 1;"