#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


## create table t09_0012
echo "create table t09_0012(c int)" | $MYSQL_CLIENT_CONNECT
# there will be 2 blocks after two insertions, each block contains 2 rows
echo "insert into t09_0012 values(1), (2)" | $MYSQL_CLIENT_CONNECT
echo "insert into t09_0012 values(3), (4)" | $MYSQL_CLIENT_CONNECT


# expects "partitions_scanned: 2"
echo "explain select * from t09_0012" | mysql -h127.0.0.1 -P3307 -uroot -s
# expects "partitions_scanned: 1"
echo "explain select * from t09_0012 limit 1" | mysql -h127.0.0.1 -P3307 -uroot -s
# expects "partitions_scanned: 1"
echo "explain select * from t09_0012 limit 2" | mysql -h127.0.0.1 -P3307 -uroot -s
# expects "partitions_scanned: 2"
echo "explain select * from t09_0012 limit 3" | mysql -h127.0.0.1 -P3307 -uroot -s
# expects "partitions_scanned: 2"
echo "explain select * from t09_0012 limit 4" | mysql -h127.0.0.1 -P3307 -uroot -s

echo "explain select * from t09_0012 limit 0" | mysql -h127.0.0.1 -P3307 -uroot -s
## Drop table.
echo "drop table  t09_0012" | $MYSQL_CLIENT_CONNECT
