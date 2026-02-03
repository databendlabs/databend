#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

run_bendsql() {
  cat <<SQL | $BENDSQL_CLIENT_CONNECT
$1
SQL
}

export TEST_USER_NAME="u1"
export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql --user=u1 --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
export USER_B_CONNECT="bendsql --user=b --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
export RM_UUID="sed -E ""s/[-a-z0-9]{32,36}/UUID/g"""

run_bendsql "
DROP TABLE IF EXISTS test_table;
DROP USER IF EXISTS u1;
DROP STAGE IF EXISTS s2;
DROP STAGE IF EXISTS s1;
CREATE STAGE s2;
CREATE TABLE test_table (
    id INTEGER,
    name VARCHAR,
    age INT
);
INSERT INTO test_table (id,name,age)
VALUES
    (1,'2',3), (4,'5',6),
    (7,'8',9), (10,'11',12),
    (13,'14',15), (16,'17',18),
    (19,'20',21), (22,'23',24),
    (25,'26',27), (28,'29',30);
"

STAGE_DIR=/tmp/copy_into_stage2

rm -rf "$STAGE_DIR"
run_bendsql "
CREATE STAGE s1 url = 'fs:///$STAGE_DIR/' FILE_FORMAT = (type = CSV);
CREATE USER u1 IDENTIFIED BY 'password';
GRANT INSERT, SELECT ON default.test_table TO u1;
"
echo "==== check internal stage write priv ==="
echo "copy into @s2 from test_table FILE_FORMAT = (type = CSV);" | $TEST_USER_CONNECT | $RM_UUID | cut -d$'\t' -f1,2
echo "grant Write on stage s2 to 'u1'" | $BENDSQL_CLIENT_CONNECT
echo "copy into @s2 from test_table FILE_FORMAT = (type = CSV);" | $TEST_USER_CONNECT | $RM_UUID | cut -d$'\t' -f1,2
echo "copy into @s2 from test_table FILE_FORMAT = (type = CSV);" | $TEST_USER_CONNECT | $RM_UUID | cut -d$'\t' -f1,2
echo "list @s2;" | $BENDSQL_CLIENT_CONNECT | wc -l | sed 's/ //g'

echo "==== check external stage priv ==="
echo "copy into @s1/csv/ from test_table FILE_FORMAT = (type = CSV);" | $TEST_USER_CONNECT | $RM_UUID | cut -d$'\t' -f1,2
echo "grant write on stage s1 to 'u1'" | $BENDSQL_CLIENT_CONNECT
echo "copy into @s1/csv/ from test_table FILE_FORMAT = (type = CSV);" | $TEST_USER_CONNECT | $RM_UUID | cut -d$'\t' -f1,2
echo "copy into test_table from @s1/csv/ FILE_FORMAT = (type = CSV skip_header = 0) force=true;" | $TEST_USER_CONNECT | $RM_UUID
echo "grant read on stage s1 to 'u1'" | $BENDSQL_CLIENT_CONNECT
echo "copy into test_table from @s1/csv/ FILE_FORMAT = (type = CSV skip_header = 0) force=true;" | $TEST_USER_CONNECT | $RM_UUID

echo "==== check internal stage read priv ==="
echo "truncate table test_table;" | $BENDSQL_CLIENT_CONNECT
echo "copy into test_table from @s2 FILE_FORMAT = (type = CSV skip_header = 0) force=true;" | $TEST_USER_CONNECT | $RM_UUID
echo "grant Read on stage s2 to 'u1'" | $BENDSQL_CLIENT_CONNECT
echo "copy into test_table from @s2 FILE_FORMAT = (type = CSV skip_header = 0) force=true;" | $TEST_USER_CONNECT | $RM_UUID

echo "remove @s2;" | $TEST_USER_CONNECT
run_bendsql "
remove @s1;
drop STAGE s2;
drop STAGE s1;
drop table test_table;
"

echo "=== check presign ==="
run_bendsql "
drop stage if exists presign_stage;
CREATE STAGE presign_stage;
"

# Most arguments is the same with previous, except:
# -X PUT: Specify the http method
run_bendsql "
grant Write, Read on stage presign_stage to 'u1';
revoke Write on stage presign_stage from 'u1';
"
curl -s -w "%{http_code}\n" -X PUT -o /dev/null -H Content-Type:application/octet-stream "$(echo "PRESIGN UPLOAD @presign_stage/hello_world.txt CONTENT_TYPE='application/octet-stream'" | $TEST_USER_CONNECT)"

echo "revoke Read on stage presign_stage from 'u1'" | $BENDSQL_CLIENT_CONNECT
curl -s -w "%{http_code}\n" -o /dev/null "$(echo "PRESIGN @presign_stage/hello_word.txt" | $TEST_USER_CONNECT)"

run_bendsql "
drop stage if exists s3;
create stage s3;
"
echo "remove @s3;" | $TEST_USER_CONNECT
echo "grant write on stage s3 to u1" | $BENDSQL_CLIENT_CONNECT
echo "remove @s3;" | $TEST_USER_CONNECT
echo "copy into '@s3/a b' from (select 2);" | $TEST_USER_CONNECT | $RM_UUID | cut -d$'\t' -f1,2

echo "grant select on system.* to u1" | $BENDSQL_CLIENT_CONNECT

echo "select * from @s3" | $TEST_USER_CONNECT
echo "select * from infer_schema(location => '@s3')" | $TEST_USER_CONNECT
echo "select * from list_stage(location => '@s3')" | $TEST_USER_CONNECT
echo "select * from inspect_parquet('@s3')" | $TEST_USER_CONNECT

echo "grant read on stage s3 to u1" | $BENDSQL_CLIENT_CONNECT

echo "select * from @s3" | $TEST_USER_CONNECT
echo "select 1 from infer_schema(location => '@s3')" | $TEST_USER_CONNECT
echo "select 1 from list_stage(location => '@s3')" | $TEST_USER_CONNECT
echo "select 1 from inspect_parquet('@s3')" | $TEST_USER_CONNECT

echo "=== check external location ==="
rm -rf /tmp/00_0012
mkdir -p /tmp/00_0012
cat <<EOF >/tmp/00_0012/i0.csv
1,1
2,2
EOF

echo "drop table if exists t" | $BENDSQL_CLIENT_CONNECT
echo "select \$1, \$2 from 'fs:///tmp/00_0012/' (FILE_FORMAT => 'CSV')" | $TEST_USER_CONNECT
echo "create table t(c1 int, c2 int)" | $BENDSQL_CLIENT_CONNECT
echo "grant select, insert on default.t to u1" | $BENDSQL_CLIENT_CONNECT
echo "copy into t from 'fs:///tmp/00_0012/' FILE_FORMAT = (type = CSV);" | $TEST_USER_CONNECT
echo "select * from t" | $BENDSQL_CLIENT_CONNECT

echo "=== check access user's local stage ==="
# file no need to check privileges
curl -s -w "%{http_code}\n" -X PUT -o /dev/null -H Content-Type:application/octet-stream "$(echo "PRESIGN UPLOAD @~/hello_world.txt CONTENT_TYPE='application/octet-stream'" | $TEST_USER_CONNECT)"

## clean ups
run_bendsql "
drop stage if exists presign_stage;
drop stage if exists s3;
drop user u1;
drop table if exists t;
"

run_bendsql "
drop user if exists b;
create user b identified by '$TEST_USER_PASSWORD';
drop table if exists t;
create table t(id int);
grant insert, delete on default.t to b;
"

cat <<EOF >/tmp/00_0012/i1.csv
1
2
EOF

echo "insert into t select \$1 from 'fs:///tmp/00_0020/' (FILE_FORMAT => 'CSV');" | $USER_B_CONNECT

echo "grant select on system.* to b" | $BENDSQL_CLIENT_CONNECT

echo "insert into t select \$1 from 'fs:///tmp/00_0020/' (FILE_FORMAT => 'CSV');" | $USER_B_CONNECT

run_bendsql "
drop table if exists t;
drop user if exists b;
"
rm -rf /tmp/00_0012
