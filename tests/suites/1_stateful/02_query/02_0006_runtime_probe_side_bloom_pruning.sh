#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


# As currently, runtime bloom pruning (probe side) only support non-blocking block reader
# this case must be tested with non-blocking storage type (minio / s3 etc, not local fs)

echo "create or replace database rt_bloom" | $BENDSQL_CLIENT_CONNECT

echo "create table rt_bloom.probe(c uint64) as select * from numbers(100000)" |  $BENDSQL_CLIENT_CONNECT
echo "create table rt_bloom.build(c uint64)" |  $BENDSQL_CLIENT_CONNECT

echo "The probe table should consist of 2 blocks"
echo "select block_count from fuse_snapshot('rt_bloom','probe')" |  $BENDSQL_CLIENT_CONNECT


echo "insert into rt_bloom.build values(50)" |  $BENDSQL_CLIENT_CONNECT


echo "runtime range filter should work, one of the blocks should be pruned by range filter"
echo "explain analyze select * from rt_bloom.probe  inner join rt_bloom.build on probe.c = build.c " \
  |  $BENDSQL_CLIENT_CONNECT | grep "parts pruned by"



echo "delete from rt_bloom.probe where c = 50" |  $BENDSQL_CLIENT_CONNECT;
echo "runtime bloom filter should work, another block should be pruned by bloom filter"
echo "explain analyze select * from rt_bloom.probe  inner join rt_bloom.build on probe.c = build.c " \
  |  $BENDSQL_CLIENT_CONNECT | grep "parts pruned by"

echo "DROP TABLE rt_bloom.probe" | $BENDSQL_CLIENT_CONNECT
echo "DROP TABLE rt_bloom.build" | $BENDSQL_CLIENT_CONNECT
echo "drop database rt_bloom" | $BENDSQL_CLIENT_CONNECT
