#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_NAME="u1"
export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql_connect_user u1 password -A"
export USER_B_CONNECT="bendsql_connect_user b password -A"
export RM_UUID="sed -E ""s/[-a-z0-9]{32,36}/UUID/g"""

echo "drop table if exists test_table;" | bendsql_connect_root
echo "drop user if exists u1;" | bendsql_connect_root
echo "drop STAGE if exists s2;" | bendsql_connect_root
echo "drop STAGE if exists s1;" | bendsql_connect_root
echo "CREATE STAGE s2;" | bendsql_connect_root

echo "CREATE TABLE test_table (
    id INTEGER,
    name VARCHAR,
    age INT
);" | bendsql_connect_root

for i in $(seq 1 10); do
    echo "insert into test_table (id,name,age) values(1,'2',3), (4, '5', 6);" | bendsql_connect_root
done

STAGE_DIR=/tmp/copy_into_stage2

rm -rf "$STAGE_DIR"
echo "create stage s1 url = 'fs:///$STAGE_DIR/' FILE_FORMAT = (type = CSV)" | bendsql_connect_root

echo "create user u1 identified by 'password';" | bendsql_connect_root
echo "grant insert on default.test_table to u1;" | bendsql_connect_root
echo "==== check internal stage write priv ==="
echo "copy into @s2 from test_table FILE_FORMAT = (type = CSV);" | $TEST_USER_CONNECT | $RM_UUID | cut -d$'\t' -f1,2
echo "grant Write on stage s2 to 'u1'" | bendsql_connect_root
echo "copy into @s2 from test_table FILE_FORMAT = (type = CSV);" | $TEST_USER_CONNECT | $RM_UUID | cut -d$'\t' -f1,2
echo "grant select on default.test_table to u1;" | bendsql_connect_root
echo "copy into @s2 from test_table FILE_FORMAT = (type = CSV);" | $TEST_USER_CONNECT | $RM_UUID | cut -d$'\t' -f1,2
echo "list @s2;" | bendsql_connect_root | wc -l | sed 's/ //g'

echo "==== check external stage priv ==="
echo "copy into @s1/csv/ from test_table FILE_FORMAT = (type = CSV);" | $TEST_USER_CONNECT | $RM_UUID | cut -d$'\t' -f1,2
echo "grant write on stage s1 to 'u1'" | bendsql_connect_root
echo "copy into @s1/csv/ from test_table FILE_FORMAT = (type = CSV);" | $TEST_USER_CONNECT | $RM_UUID | cut -d$'\t' -f1,2
echo "copy into test_table from @s1/csv/ FILE_FORMAT = (type = CSV skip_header = 0) force=true;" | $TEST_USER_CONNECT | $RM_UUID
echo "grant read on stage s1 to 'u1'" | bendsql_connect_root
echo "copy into test_table from @s1/csv/ FILE_FORMAT = (type = CSV skip_header = 0) force=true;" | $TEST_USER_CONNECT | $RM_UUID

echo "==== check internal stage read priv ==="
echo "truncate table test_table;" | bendsql_connect_root
echo "copy into test_table from @s2 FILE_FORMAT = (type = CSV skip_header = 0) force=true;" | $TEST_USER_CONNECT | $RM_UUID
echo "grant Read on stage s2 to 'u1'" | bendsql_connect_root
echo "copy into test_table from @s2 FILE_FORMAT = (type = CSV skip_header = 0) force=true;" | $TEST_USER_CONNECT | $RM_UUID

echo "==== check schema evolution alter priv ==="
echo "drop table if exists se_table;" | bendsql_connect_root
echo "drop table if exists se_query_table;" | bendsql_connect_root
echo "drop stage if exists s_schema_evolution;" | bendsql_connect_root
echo "create table se_table(a int);" | bendsql_connect_root
echo "alter table se_table set options(enable_schema_evolution = true);" | bendsql_connect_root
echo "create table se_query_table(a int);" | bendsql_connect_root
echo "alter table se_query_table set options(enable_schema_evolution = true);" | bendsql_connect_root
echo "create stage s_schema_evolution;" | bendsql_connect_root
echo "copy into @s_schema_evolution from (select 1 as a, 2 as b) file_format=(type=ndjson);" | bendsql_connect_root >/dev/null
echo "grant read on stage s_schema_evolution to u1;" | bendsql_connect_root
echo "grant insert on default.se_table to u1;" | bendsql_connect_root
echo "copy into se_table from @s_schema_evolution/ file_format=(type=ndjson missing_field_as=field_default) force=true;" | $TEST_USER_CONNECT | $RM_UUID
echo "grant alter on default.se_table to u1;" | bendsql_connect_root
echo "copy into se_table from @s_schema_evolution/ file_format=(type=ndjson missing_field_as=field_default) force=true;" | $TEST_USER_CONNECT | $RM_UUID
echo "select a, b from se_table;" | bendsql_connect_root
echo "grant insert on default.se_query_table to u1;" | bendsql_connect_root
echo "copy into se_query_table from (select to_int32(\$1:a) from @s_schema_evolution) file_format=(type=ndjson) force=true;" | $TEST_USER_CONNECT | $RM_UUID
echo "select * from se_query_table;" | bendsql_connect_root
echo "drop table se_table;" | bendsql_connect_root
echo "drop table se_query_table;" | bendsql_connect_root
echo "drop stage s_schema_evolution;" | bendsql_connect_root

echo "remove @s2;" | $TEST_USER_CONNECT
echo "remove @s1;" | bendsql_connect_root
echo "drop STAGE s2;" | bendsql_connect_root
echo "drop STAGE s1;" | bendsql_connect_root
echo "drop table test_table;" | bendsql_connect_root

echo "=== check presign ==="
echo "drop stage if exists presign_stage" | bendsql_connect_root

echo "CREATE STAGE presign_stage;" | bendsql_connect_root

# Most arguments is the same with previous, except:
# -X PUT: Specify the http method
echo "grant Write, Read on stage presign_stage to 'u1'" | bendsql_connect_root
echo "revoke Write on stage presign_stage from 'u1'" | bendsql_connect_root
curl -s -w "%{http_code}\n" -X PUT -o /dev/null -H Content-Type:application/octet-stream "$(echo "PRESIGN UPLOAD @presign_stage/hello_world.txt CONTENT_TYPE='application/octet-stream'" | $TEST_USER_CONNECT)"

echo "revoke Read on stage presign_stage from 'u1'" | bendsql_connect_root
curl -s -w "%{http_code}\n" -o /dev/null "$(echo "PRESIGN @presign_stage/hello_word.txt" | $TEST_USER_CONNECT)"

echo "drop stage if exists s3" | bendsql_connect_root

echo "create stage s3;" | bendsql_connect_root
echo "remove @s3;" | $TEST_USER_CONNECT
echo "grant write on stage s3 to u1" | bendsql_connect_root
echo "remove @s3;" | $TEST_USER_CONNECT
echo "copy into '@s3/a b' from (select 2);" | $TEST_USER_CONNECT | $RM_UUID | cut -d$'\t' -f1,2

echo "grant select on system.* to u1" | bendsql_connect_root

echo "select * from @s3" | $TEST_USER_CONNECT
echo "select * from infer_schema(location => '@s3')" | $TEST_USER_CONNECT
echo "select * from list_stage(location => '@s3')" | $TEST_USER_CONNECT
echo "select * from inspect_parquet('@s3')" | $TEST_USER_CONNECT

echo "grant read on stage s3 to u1" | bendsql_connect_root

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

echo "drop table if exists t" | bendsql_connect_root
echo "select \$1, \$2 from 'fs:///tmp/00_0012/' (FILE_FORMAT => 'CSV')" | $TEST_USER_CONNECT
echo "create table t(c1 int, c2 int)" | bendsql_connect_root
echo "grant select, insert on default.t to u1" | bendsql_connect_root
echo "copy into t from 'fs:///tmp/00_0012/' FILE_FORMAT = (type = CSV);" | $TEST_USER_CONNECT
echo "select * from t" | bendsql_connect_root

echo "=== check access user's local stage ==="
# file no need to check privileges
curl -s -w "%{http_code}\n" -X PUT -o /dev/null -H Content-Type:application/octet-stream "$(echo "PRESIGN UPLOAD @~/hello_world.txt CONTENT_TYPE='application/octet-stream'" | $TEST_USER_CONNECT)"

## clean ups
echo "drop stage if exists presign_stage" | bendsql_connect_root
echo "drop stage if exists s3" | bendsql_connect_root
echo "drop user u1" | bendsql_connect_root
echo "drop table if exists t" | bendsql_connect_root

echo "drop user if exists b" | bendsql_connect_root
echo "create user b identified by '$TEST_USER_PASSWORD'" | bendsql_connect_root
echo "drop table if exists t" | bendsql_connect_root
echo "create table t(id int)" | bendsql_connect_root

echo "grant insert, delete on default.t to b" | bendsql_connect_root

cat <<EOF >/tmp/00_0012/i1.csv
1
2
EOF

echo "insert into t select \$1 from 'fs:///tmp/00_0020/' (FILE_FORMAT => 'CSV');" | $USER_B_CONNECT

echo "grant select on system.* to b" | bendsql_connect_root

echo "insert into t select \$1 from 'fs:///tmp/00_0020/' (FILE_FORMAT => 'CSV');" | $USER_B_CONNECT

echo "drop table if exists t" | bendsql_connect_root
echo "drop user if exists b" | bendsql_connect_root
rm -rf /tmp/00_0012
