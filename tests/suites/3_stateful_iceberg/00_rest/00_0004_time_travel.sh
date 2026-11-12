#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

db="iceberg_time_travel"

snapshot_ids() {
	echo "USE CATALOG iceberg_rest; SELECT snapshot_id FROM iceberg_snapshot('${db}', 't') $1;" | bendsql_connect_root
}

cat <<EOF | bendsql_connect_root
CREATE CATALOG IF NOT EXISTS iceberg_rest TYPE = ICEBERG CONNECTION = (
    TYPE = 'rest' ADDRESS = 'http://localhost:8181' warehouse = 's3://warehouse/demo/' "s3.endpoint" = 'http://localhost:9000' "s3.access-key-id" = 'admin' "s3.secret-access-key" = 'password' "s3.region" = 'us-east-1'
);
USE CATALOG iceberg_rest;
DROP TABLE IF EXISTS ${db}.t;
DROP DATABASE IF EXISTS ${db};
CREATE DATABASE ${db};
CREATE TABLE ${db}.t (a bigint);
INSERT INTO ${db}.t VALUES (1);
EOF

first=$(snapshot_ids "")

echo "USE CATALOG iceberg_rest; INSERT INTO ${db}.t VALUES (2);" | bendsql_connect_root >/dev/null
second=$(snapshot_ids "WHERE snapshot_id != ${first}")

echo "USE CATALOG iceberg_rest; INSERT INTO ${db}.t VALUES (3);" | bendsql_connect_root >/dev/null

# The snapshot id resolved by the planner has to reach the table the scan pipeline is rebuilt
# from, otherwise every `AT (SNAPSHOT => ...)` scan reads the current snapshot.
cat <<EOF | bendsql_connect_root
USE CATALOG iceberg_rest;
SELECT count(*) FROM ${db}.t;
SELECT count(*) FROM ${db}.t AT (SNAPSHOT => '${first}');
SELECT * FROM ${db}.t AT (SNAPSHOT => '${second}') ORDER BY a;
SELECT count(*) FROM ${db}.t AT (TIMESTAMP => now());
EOF

# A snapshot id the table does not have is an error instead of the current data.
echo ">>>> SELECT count(*) FROM ${db}.t AT (SNAPSHOT => '1');"
echo "USE CATALOG iceberg_rest; SELECT count(*) FROM ${db}.t AT (SNAPSHOT => '1');" | bendsql_connect_root 2>&1
echo "<<<<"

cat <<EOF | bendsql_connect_root
USE CATALOG iceberg_rest;
DROP TABLE ${db}.t;
DROP DATABASE ${db};
EOF
