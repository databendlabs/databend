#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

SEGMENTS=$(echo "select file_location from fuse_segment('default','test_vacuum2');" | $BENDSQL_CLIENT_CONNECT)
BLOCKS=$(echo "select block_location from fuse_block('default','test_vacuum2');" | $BENDSQL_CLIENT_CONNECT)

stmt "insert into test_vacuum2 values(2);"

SNAPSHOTS=$(echo "select snapshot_location from fuse_snapshot('default','test_vacuum2');" | $BENDSQL_CLIENT_CONNECT)

IFS=$'\n' read -d '' -r -a snapshots <<< "$SNAPSHOTS"
IFS=$'\n' read -d '' -r -a segments <<< "$SEGMENTS"
IFS=$'\n' read -d '' -r -a blocks <<< "$BLOCKS"
to_be_vacuumed=("${snapshots[@]}" "${segments[@]}" "${blocks[@]}")

# gc root
stmt "set data_retention_time_in_days = 2;truncate table test_vacuum2;"

stmt "insert into test_vacuum2 values(3);"

# should have 4 snapshots
query "select count(*) from fuse_snapshot('default','test_vacuum2')"

RESULTS=$(echo "set data_retention_time_in_days = 0;select * from fuse_vacuum2('default','test_vacuum2');" | $BENDSQL_CLIENT_CONNECT)
IFS=$'\n' read -d '' -r -a results <<< "$RESULTS"

# verify the vacuum result
sorted_results=($(printf "%s\n" "${results[@]}" | sort))
sorted_to_be_vacuumed=($(printf "%s\n" "${to_be_vacuumed[@]}" | sort))

if [ "$(printf "%s" "${sorted_results[@]}")" != "$(printf "%s" "${sorted_to_be_vacuumed[@]}")" ]; then
    echo "Vacuum failed"
    echo "Results array: ${sorted_results[@]}"
    echo "To be vacuumed array: ${sorted_to_be_vacuumed[@]}"
    exit 1
fi

# remain two snapshots
query "select count(*) from fuse_snapshot('default','test_vacuum2')"

# verify the data
query "select * from test_vacuum2;"

# restore default value
stmt "set data_retention_time_in_days = 1;"