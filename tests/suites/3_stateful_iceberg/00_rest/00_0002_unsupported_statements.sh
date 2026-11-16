#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

db="iceberg_noop_ddl"

# Run one statement in the iceberg catalog and print the error it is expected to return.
error_of() {
	echo ">>>> $1"
	printf 'USE CATALOG iceberg_rest;\n%s\n' "$1" | bendsql_connect_root 2>&1
	echo "<<<<"
}

cat <<EOF | bendsql_connect_root
CREATE CATALOG IF NOT EXISTS iceberg_rest TYPE = ICEBERG CONNECTION = (
    TYPE = 'rest' ADDRESS = 'http://localhost:8181' warehouse = 's3://warehouse/demo/' "s3.endpoint" = 'http://localhost:9000' "s3.access-key-id" = 'admin' "s3.secret-access-key" = 'password' "s3.region" = 'us-east-1'
);
USE CATALOG iceberg_rest;
DROP TABLE IF EXISTS ${db}.t;
DROP DATABASE IF EXISTS ${db};
CREATE DATABASE ${db};
CREATE TABLE ${db}.t (a bigint, b string);
INSERT INTO ${db}.t VALUES (1, 'x'), (2, 'y'), (3, 'z');
SELECT count(*) FROM ${db}.t;
EOF

# These statements used to report success: INSERT OVERWRITE appended the rows instead of
# replacing them, and TRUNCATE TABLE / ALTER TABLE left every row and the schema untouched.
error_of "INSERT OVERWRITE INTO ${db}.t SELECT 9, 'z'"
error_of "TRUNCATE TABLE ${db}.t"
error_of "ALTER TABLE ${db}.t ADD COLUMN c int"
error_of "ALTER TABLE ${db}.t DROP COLUMN b"

# Rejected statements must not have changed the table: the rows and the schema are untouched.
cat <<EOF | bendsql_connect_root
USE CATALOG iceberg_rest;
SELECT count(*) FROM ${db}.t;
SELECT * FROM ${db}.t ORDER BY a;
DESC ${db}.t;
DROP TABLE ${db}.t;
DROP DATABASE ${db};
EOF
