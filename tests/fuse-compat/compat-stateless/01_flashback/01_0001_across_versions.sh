#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# this case is supposed to run with the current version of query,
# after fuse-compat/compat-logictest/01_meta_compression/ been run with the old version of query

##########################################
# test flashback across version boundary #
##########################################

echo "suite: flashback across version boundary"
# current situation:
# - we have inserted 3 rows into fuse_test_flashback, which produced 3 snapshots
# - and the content of the table is {1,2,3}


echo "checking that 3 snapshots exist"
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

###########################################################
# test compaction & fuse_snapshot across version boundary #
# issue 11204                                             #
###########################################################

echo "suite: test compaction & fuse_snapshot across version boundary"

# current situation:
# - we have inserted 2 rows into fuse_test_compaction, which produced 2 snapshots
# - the content of the table is {1,2}
# - the snapshots are s1 {1}, s2 {1,2}


echo "checking that 2 snapshots of version 2 exist"
echo "select count() c, format_version v from fuse_snapshot('default', 'fuse_test_compaction') group by v order by c" | $MYSQL_CLIENT_CONNECT

# grab the current last snapshot (s2)
FST_SNAPSHOT_S2_ID=$(echo "select snapshot_id from fuse_snapshot('default','fuse_test_compaction') limit 1" | $MYSQL_CLIENT_CONNECT)
# grab s2's location
FST_SNAPSHOT_S2_ID_LOC=$(echo "select snapshot_location from fuse_snapshot('default','fuse_test_compaction') limit 1" | $MYSQL_CLIENT_CONNECT)

# compact the table
echo "doing compact"
echo "optimize table fuse_test_compaction compact" | $MYSQL_CLIENT_CONNECT

# verify the following cases:

echo "checking that after compaction the table still contains {1,2}"
echo "select c from fuse_test_compaction order by c" | $MYSQL_CLIENT_CONNECT

echo "checking that after compaction, 2 snapshots of version 2, and 1 snapshot of version 3 exist"
echo "select count() c, format_version v from fuse_snapshot('default', 'fuse_test_compaction') group by v order by c " | $MYSQL_CLIENT_CONNECT

echo "checking the version and location of snapshots s2 is correct"
echo "select snapshot_location='${FST_SNAPSHOT_S2_ID_LOC}', format_version=2 from fuse_snapshot('default', 'fuse_test_compaction') where snapshot_id='$FST_SNAPSHOT_S2_ID'" | $MYSQL_CLIENT_CONNECT

# flash back to s2
echo "alter table fuse_test_compaction flashback to (snapshot => '$FST_SNAPSHOT_S2_ID')" | $MYSQL_CLIENT_CONNECT
echo "checking that flashback works as expected (to s2)"
echo "select snapshot_location='${FST_SNAPSHOT_S2_ID_LOC}', format_version=2 from fuse_snapshot('default', 'fuse_test_compaction') limit 1" | $MYSQL_CLIENT_CONNECT

echo "checking that after flashback to s2,  the table contains {1,2}"
echo "select c  from fuse_test_compaction order by c" | $MYSQL_CLIENT_CONNECT


