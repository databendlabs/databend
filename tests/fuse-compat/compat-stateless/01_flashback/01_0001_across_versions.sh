#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# this case is supposed to run with the current version of query,
# after fuse-compat/compat-logictest/01_meta_compression/ been run with the old version of query

# current situation:
# - we have inserted 3 rows into fuse_test_flashback, which produced 3 snapshots
# - and the content of the table is {1,2,3}

echo "checking that 3 snapshots exit"
echo "select count(*)  from fuse_snapshot('default', 'fuse_test_flashback')" | $MYSQL_CLIENT_CONNECT

# write down 2 new rows using current version
echo "insert into fuse_test_flashback values (4)" | $MYSQL_CLIENT_CONNECT
echo "insert into fuse_test_flashback values (5)" | $MYSQL_CLIENT_CONNECT

# the table now contains five rows {1,2,3,4,5}, and 5 snapshots:
# s1 {1}, s2 {1,2}, s3 {1,2,3}, s4 {1,2,3,4}, s5 {1,2,3,4,5}
# s1, s2, s3 are of table snapshot format version 2
# s4, s5 are of table snapshot format version 3
echo "checking that 5 snapshot exist"
echo "select count(*)  from fuse_snapshot('default', 'fuse_test_flashback')" | $MYSQL_CLIENT_CONNECT

# flashback to the 2nd last snapshot
FST_SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','fuse_test_flashback') where row_count=4" | $MYSQL_CLIENT_CONNECT)
echo "alter table fuse_test_flashback flashback to (snapshot => '$FST_SNAPSHOT_ID')" | $MYSQL_CLIENT_CONNECT

echo "checking that after flashback to the 2nd last snapshot s4 which is of version 3, and now the table contains {1,2,3,4}"
echo "select c  from fuse_test_flashback order by c" | $MYSQL_CLIENT_CONNECT

FST_SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','fuse_test_flashback') where row_count=3" | $MYSQL_CLIENT_CONNECT)
echo "alter table fuse_test_flashback flashback to (snapshot => '$FST_SNAPSHOT_ID')" | $MYSQL_CLIENT_CONNECT

echo "checking that after flashback to the 3rd last snapshot s3 which is of version 3, and the table contains {1,2,3}"
echo "select c  from fuse_test_flashback order by c" | $MYSQL_CLIENT_CONNECT

# crossing the version boundary

FST_SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','fuse_test_flashback') where row_count=2" | $MYSQL_CLIENT_CONNECT)
echo "alter table fuse_test_flashback flashback to (snapshot => '$FST_SNAPSHOT_ID')" | $MYSQL_CLIENT_CONNECT

echo "checking that after flashback to the 4th last snapshot s2 which is of version 2, the table contains {1,2}"
echo "select c  from fuse_test_flashback order by c" | $MYSQL_CLIENT_CONNECT

FST_SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','fuse_test_flashback') where row_count=1" | $MYSQL_CLIENT_CONNECT)
echo "alter table fuse_test_flashback flashback to (snapshot => '$FST_SNAPSHOT_ID')" | $MYSQL_CLIENT_CONNECT

echo "checking that after flashback to the 5th last snapshot s1 which is of version 2, the table contains {1}"
echo "select c  from fuse_test_flashback order by c" | $MYSQL_CLIENT_CONNECT
