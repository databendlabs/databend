#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

echo "drop table if exists csv_null" | $MYSQL_CLIENT_CONNECT
echo "drop stage if exists csv_null" | $MYSQL_CLIENT_CONNECT

echo "CREATE TABLE csv_null
 (
     a Int Null,
     b Int Null
 );" | $MYSQL_CLIENT_CONNECT

echo "create stage csv_null url = 'fs:///tmp/' FILE_FORMAT = (type = CSV)" | $MYSQL_CLIENT_CONNECT

cat << EOF > /tmp/test_csv_null_1.csv
\N,1
EOF

cat << EOF > /tmp/test_csv_null_2.csv
null,2
EOF

echo "copy into csv_null from @csv_null/test_csv_null_1.csv" | $MYSQL_CLIENT_CONNECT
echo "copy into csv_null from @csv_null/test_csv_null_2.csv file_format = (type=CSV null_display='null') " | $MYSQL_CLIENT_CONNECT
echo "select * from csv_null order by b" | $MYSQL_CLIENT_CONNECT
