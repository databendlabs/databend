#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# set data_retention_time_in_days > 2, or this can not commit successfully
stmt "set data_retention_time_in_days = 3;insert into test_vacuum2 values(2);"

# gc root, segments and blocks that contain data '1','2' should be able to be vacuumed later
stmt "set data_retention_time_in_days = 2;truncate table test_vacuum2;"

stmt "insert into test_vacuum2 values(3);"

# 4
query "select count(*) from fuse_snapshot('default','test_vacuum2')"

# Ok
query "set data_retention_time_in_days = 0;select * from fuse_vacuum2('default','test_vacuum2')"

# 2
query "select count(*) from fuse_snapshot('default','test_vacuum2')"