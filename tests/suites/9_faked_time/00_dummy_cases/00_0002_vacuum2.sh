#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# set data_retention_time_in_days > 2, or this can not commit successfully
stmt "set data_retention_time_in_days = 3;insert into test_vacuum2 values(2);"

SNAPSHOTS=$(echo "select snapshot_location from fuse_snapshot('default','test_vacuum2');" | $BENDSQL_CLIENT_CONNECT)
SEGMENTS=$(echo "select file_location from fuse_segment('default','test_vacuum2');" | $BENDSQL_CLIENT_CONNECT)
BLOCKS=$(echo "select block_location from fuse_block('default','test_vacuum2');" | $BENDSQL_CLIENT_CONNECT)

IFS=$'\n' read -d '' -r -a snapshots <<< "$SNAPSHOTS"
IFS=$'\n' read -d '' -r -a segments <<< "$SEGMENTS"
IFS=$'\n' read -d '' -r -a blocks <<< "$BLOCKS"
to_be_vacuumed=("${snapshots[@]}" "${segments[@]}" "${blocks[@]}")

# gc root, segments and blocks that contain data '1','2' should be able to be vacuumed later
stmt "set data_retention_time_in_days = 2;truncate table test_vacuum2;"

stmt "insert into test_vacuum2 values(3);"

# should have 4 snapshots
query "select count(*) from fuse_snapshot('default','test_vacuum2')"

RESULTS=$(echo "set data_retention_time_in_days = 0;select * from fuse_vacuum2('default','test_vacuum2');" | $BENDSQL_CLIENT_CONNECT)
IFS=$'\n' read -d '' -r -a results <<< "$RESULTS"

# verify the vacuum result
if [ ${#results[@]} -ne ${#to_be_vacuumed[@]} ]; then
    echo "vacuum failed"
    exit 1
fi

# remain two snapshots
query "select count(*) from fuse_snapshot('default','test_vacuum2')"

# verify the data
query "select * from test_vacuum2;"

# restore default value
stmt "set data_retention_time_in_days = 1;"