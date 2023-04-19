#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# todo describe the current state

echo "checking that 3 snapshots exits"
echo "select count(*)=3  from fuse_snapshot('default', 'fuse_test_flashback')" | $MYSQL_CLIENT_CONNECT

# write down 2 new rows
echo "insert into fuse_test_flashback values (4)" | $MYSQL_CLIENT_CONNECT
echo "insert into fuse_test_flashback values (5)" | $MYSQL_CLIENT_CONNECT

echo "checking that 5 snapshot exist"
echo "select count(*)=5  from fuse_snapshot('default', 'fuse_test_flashback')" | $MYSQL_CLIENT_CONNECT

FST_SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','fuse_test_flashback') where row_count=4" | $MYSQL_CLIENT_CONNECT)
echo "alter table fuse_test_flashback flashback to (snapshot => '$FST_SNAPSHOT_ID')" | $MYSQL_CLIENT_CONNECT

echo "checking that after flashback to the 2nd last snapshot which is of version 3, the table is as expected"
echo "select c  from fuse_test_flashback order by c" | $MYSQL_CLIENT_CONNECT

FST_SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','fuse_test_flashback') where row_count=3" | $MYSQL_CLIENT_CONNECT)
echo "alter table fuse_test_flashback flashback to (snapshot => '$FST_SNAPSHOT_ID')" | $MYSQL_CLIENT_CONNECT

echo "checking that after flashback to the 3nd last snapshot which is of version 3, the table is as expected"
echo "select c  from fuse_test_flashback order by c" | $MYSQL_CLIENT_CONNECT

# crossing the version boundary

FST_SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','fuse_test_flashback') where row_count=2" | $MYSQL_CLIENT_CONNECT)
echo "alter table fuse_test_flashback flashback to (snapshot => '$FST_SNAPSHOT_ID')" | $MYSQL_CLIENT_CONNECT

echo "checking that after flashback to the 3nd last snapshot which is of version 2, the table is as expected"
echo "select c  from fuse_test_flashback order by c" | $MYSQL_CLIENT_CONNECT

FST_SNAPSHOT_ID=$(echo "select snapshot_id from fuse_snapshot('default','fuse_test_flashback') where row_count=1" | $MYSQL_CLIENT_CONNECT)
echo "alter table fuse_test_flashback flashback to (snapshot => '$FST_SNAPSHOT_ID')" | $MYSQL_CLIENT_CONNECT
echo "checking that after flashback to the 3nd last snapshot which is of version 2, the table is as expected"
echo "select c  from fuse_test_flashback order by c" | $MYSQL_CLIENT_CONNECT
