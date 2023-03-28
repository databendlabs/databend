#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh


# Should be <root>/tests/data/
DATADIR=$(realpath $CURDIR/../../../data/)

echo "drop table if exists test_max_files_force_true" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists test_max_files_force_false" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE test_max_files_force_true (
  id INT,
  c1 INT
) ENGINE=FUSE;" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE test_max_files_force_false (
  id INT,
  c1 INT
) ENGINE=FUSE;" | $MYSQL_CLIENT_CONNECT

rm -rf /tmp/05_02_06
mkdir -p /tmp/05_02_06

cat << EOF > /tmp/05_02_06/force_true.csv
1,1
2,2
EOF

cat << EOF > /tmp/05_02_06/force_false.csv
3,3
4,4
EOF

cat << EOF > /tmp/05_02_06/f3.csv
5,5
6,6
EOF

echo "copy into test_max_files_force_true from 'fs:///tmp/05_02_06/' FILE_FORMAT = (type = CSV) max_files=2 force=true" | $MYSQL_CLIENT_CONNECT
echo "select count(*) from test_max_files_force_true" | $MYSQL_CLIENT_CONNECT

echo "copy into test_max_files_force_true from 'fs:///tmp/05_02_06/' FILE_FORMAT = (type = CSV) max_files=2 force=true" | $MYSQL_CLIENT_CONNECT
echo "select count(*) from test_max_files_force_true" | $MYSQL_CLIENT_CONNECT

echo "copy into test_max_files_force_true from 'fs:///tmp/05_02_06/' FILE_FORMAT = (type = CSV) max_files=2 force=true" | $MYSQL_CLIENT_CONNECT
echo "select count(*) from test_max_files_force_true" | $MYSQL_CLIENT_CONNECT

echo "copy into test_max_files_force_false from 'fs:///tmp/05_02_06/' FILE_FORMAT = (type = CSV) max_files=2 force=false" | $MYSQL_CLIENT_CONNECT
echo "select count(*) from test_max_files_force_false" | $MYSQL_CLIENT_CONNECT

echo "copy into test_max_files_force_false from 'fs:///tmp/05_02_06/' FILE_FORMAT = (type = CSV) max_files=2 force=false" | $MYSQL_CLIENT_CONNECT
echo "select count(*) from test_max_files_force_false" | $MYSQL_CLIENT_CONNECT

echo "copy into test_max_files_force_false from 'fs:///tmp/05_02_06/' FILE_FORMAT = (type = CSV) max_files=2 force=false" | $MYSQL_CLIENT_CONNECT
echo "select count(*) from test_max_files_force_false" | $MYSQL_CLIENT_CONNECT

echo "drop table if exists test_max_files_force_true" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists test_max_files_force_false" | $MYSQL_CLIENT_CONNECT
