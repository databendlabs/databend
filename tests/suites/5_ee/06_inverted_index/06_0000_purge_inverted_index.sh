#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


### test for purging inverted index files, using transient table ###

echo "drop database if exists db_purge_inverted_index" | $BENDSQL_CLIENT_CONNECT

echo "CREATE DATABASE db_purge_inverted_index" | $BENDSQL_CLIENT_CONNECT

TEST_DB="db_purge_inverted_index"

echo "create or replace stage test_purge_ii url='fs:///tmp/purge_inverted_index/'" | $BENDSQL_CLIENT_CONNECT

# uncomment this line to clean up the stage (for local diagnostic only)
# rm -rf /tmp/purge_inverted_index/*

# create transient table with 2 inverted index, at the location of @test_purge_ii
echo "CREATE or replace transient TABLE  ${TEST_DB}.customer_feedback ( comment_title VARCHAR NULL, comment_body VARCHAR NULL ) 'fs:///tmp/purge_inverted_index/'" | $BENDSQL_CLIENT_CONNECT
echo "CREATE INVERTED INDEX idx1 ON ${TEST_DB}.customer_feedback(comment_title)" | $BENDSQL_CLIENT_CONNECT
echo "CREATE INVERTED INDEX idx2 ON ${TEST_DB}.customer_feedback(comment_body)" | $BENDSQL_CLIENT_CONNECT



echo "###################"
echo "###1st insertion###"
echo "###################"

echo "insert into ${TEST_DB}.customer_feedback values('a', 'b')" | $BENDSQL_CLIENT_CONNECT
echo "== number of snapshots (expects 3)=="
# 1 snapshot for the init insertion, other 2 snapshots are created by inverted index refreshment.
# NOTE:
# - the inverted index refreshment happens AFTER auto purge of transient table,
# - the inverted index refreshment will create one new snapshot for each index (2 snapshot in our case, since there are 2 indexes)
echo "select snapshot_id, previous_snapshot_id from fuse_snapshot('db_purge_inverted_index', 'customer_feedback') limit 100" | $BENDSQL_CLIENT_CONNECT | wc -l
# uncomment this line to show the details (for local diagnostic only)
#echo "select snapshot_id, previous_snapshot_id from fuse_snapshot('db_purge_inverted_index', 'customer_feedback') limit 100" | $BENDSQL_CLIENT_CONNECT

echo "== number of invert index files (expects 2) =="
# NOTE: since there are 1 block, 2 inverted indexes.
echo "list @test_purge_ii PATTERN = '.*/_i_i/.*.index';" | $BENDSQL_CLIENT_CONNECT | wc -l
# uncomment this line to show the details (for local diagnostic only)
#echo "list @test_purge_ii PATTERN = '.*/_i_i/.*.index';" | $BENDSQL_CLIENT_CONNECT




echo "###################"
echo "###2nd insertion###"
echo "###################"

echo "insert into ${TEST_DB}.customer_feedback values('a', 'b')" | $BENDSQL_CLIENT_CONNECT
echo "== number of snapshots (expects 3)=="
# NOTE:
# - since previous snapshots should be purged,
# - and 1 snapshot created for this new insertion, other 2 snapshots are created by inverted index refreshment.
echo "select snapshot_id, previous_snapshot_id from fuse_snapshot('db_purge_inverted_index', 'customer_feedback') limit 100" | $BENDSQL_CLIENT_CONNECT | wc -l
# uncomment this line to show the details (for local diagnostic only)
#   it will shows that the oldest snapshot is the previous_snapshot of the latest snapshot before 2nd insertion
#echo "select snapshot_id, previous_snapshot_id from fuse_snapshot('db_purge_inverted_index', 'customer_feedback') limit 100" | $BENDSQL_CLIENT_CONNECT

echo "== number of invert index files (expects 4) =="
# NOTE: since there are 2 blocks now, each of them will have 2 inverted indexes.
echo "list @test_purge_ii PATTERN = '.*/_i_i/.*.index';" | $BENDSQL_CLIENT_CONNECT | wc -l
# uncomment this line to show the details (for local diagnostic only)
#echo "list @test_purge_ii PATTERN = '.*/_i_i/.*.index';" | $BENDSQL_CLIENT_CONNECT


echo "###################"
echo "###3nd insertion###"
echo "###################"

echo "insert into ${TEST_DB}.customer_feedback values('a', 'b')" | $BENDSQL_CLIENT_CONNECT

echo "== number of snapshots (expects 3)=="
echo "select snapshot_id, previous_snapshot_id from fuse_snapshot('db_purge_inverted_index', 'customer_feedback') limit 100" | $BENDSQL_CLIENT_CONNECT | wc -l
# uncomment this line to show the details (for local diagnostic only)
#   it will shows that the oldest snapshot is the previous_snapshot of the latest snapshot before 3nd insertion
#echo "select snapshot_id, previous_snapshot_id from fuse_snapshot('db_purge_inverted_index', 'customer_feedback') limit 100" | $BENDSQL_CLIENT_CONNECT

echo "== number of invert index files (expects 6) =="
# NOTE: since there are 2 blocks now, each of them will have 2 inverted indexes.
echo "list @test_purge_ii PATTERN = '.*/_i_i/.*.index';" | $BENDSQL_CLIENT_CONNECT | wc -l
# uncomment this line to show the details (for local diagnostic only)
#echo "list @test_purge_ii PATTERN = '.*/_i_i/.*.index';" | $BENDSQL_CLIENT_CONNECT


echo "###################"
echo "####compaction#####"
echo "###################"

echo "optimize table ${TEST_DB}.customer_feedback compact" | $BENDSQL_CLIENT_CONNECT
echo "== number of snapshots (expects 1)=="
# NOTE: since previous snapshot will be purged(transient table), but inverted index refreshment is NOT executed after compaction
echo "select snapshot_id, previous_snapshot_id from fuse_snapshot('db_purge_inverted_index', 'customer_feedback') limit 100" | $BENDSQL_CLIENT_CONNECT | wc -l

echo "== number of invert index files (expects 0) =="
# NOTE: since inverted index refreshment is NOT executed after compaction, and the inverted index files
# that associated with data blocks are purged alone with the fragmented blocks.
echo "list @test_purge_ii PATTERN = '.*/_i_i/.*.index';" | $BENDSQL_CLIENT_CONNECT | wc -l
# uncomment this line to show the details (for local diagnostic only)
#echo "list @test_purge_ii PATTERN = '.*/_i_i/.*.index';" | $BENDSQL_CLIENT_CONNECT

echo "###################"
echo "####new insertion##"
echo "###################"

echo "insert into ${TEST_DB}.customer_feedback values('a', 'b')" | $BENDSQL_CLIENT_CONNECT
echo "== number of snapshots (expects 3) =="
echo "select snapshot_id, previous_snapshot_id from fuse_snapshot('db_purge_inverted_index', 'customer_feedback') limit 100" | $BENDSQL_CLIENT_CONNECT | wc -l
echo "== number of invert index files (expects 2) =="
# NOTE: the inverted index refreshment will ONLY create new indexes for the new blocks, and there is one new block and 2 indexes
echo "list @test_purge_ii PATTERN = '.*/_i_i/.*.index';" | $BENDSQL_CLIENT_CONNECT | wc -l
# for local diagnostic only
#echo "list @test_purge_ii PATTERN = '.*/_i_i/.*.index';" | $BENDSQL_CLIENT_CONNECT


echo "drop table ${TEST_DB}.customer_feedback" | $BENDSQL_CLIENT_CONNECT
echo "drop stage test_purge_ii" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists ${TEST_DB}" | $BENDSQL_CLIENT_CONNECT
